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

/// Where an upgrade is. Only the forward edges in `Self::can_advance_to`
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

/// The phases in which a row refuses other activations of its sandbox.
/// `fencing_runtime_upgrade` and `fenced_runtime_upgrade_publications` bind
/// these. Other phase lists spell them out; a test holds
/// [`UNRETIRED_UPGRADE_PHASES_SQL`] to exactly these and `verified`.
const FENCING_PHASES: [RuntimeUpgradePhase; 3] = [
    RuntimeUpgradePhase::Fenced,
    RuntimeUpgradePhase::Preserved,
    RuntimeUpgradePhase::Booting,
];

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
    pub(crate) fn fences_activation(self) -> bool {
        FENCING_PHASES.contains(&self)
    }

    /// Whether the operation is done with the sandbox, for any target state,
    /// so [`abandon_runtime_upgrade`] records it abandoned without settling
    /// it first, and the abandon RPC doesn't settle it either. A rolled-back
    /// row already restored the sandbox's disk and released its fence; what
    /// it still owes is deleting the preserved copy, which never touches the
    /// sandbox, so recovery's [`crate::db::scan::unsettled_runtime_upgrades`]
    /// still lists it and finishes that after an abandon as it would before. A
    /// fencing phase may still change the disk, and a verified one still owes
    /// the retirement and, for a stopped target, the stop. Spelled out with no
    /// wildcard, so a new phase has to choose.
    pub const fn is_done_with_the_sandbox(self) -> bool {
        match self {
            Self::RolledBack | Self::Retired => true,
            Self::Fenced | Self::Preserved | Self::Booting | Self::Verified => false,
        }
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
    /// The attempt the upgrade runs as: the opaque attempt id of the
    /// `UpgradeRuntime` request that began it, or the one the node generated
    /// for a request that named none (see [`new_runtime_upgrade_attempt_id`]).
    /// Its rollback is recorded for that attempt. `None` for an upgrade begun
    /// with attempt finality off, which is final for no attempt and is
    /// reported for none whatever the switch is when it's read, and for a row
    /// written before the node recorded attempts.
    pub attempt_id: Option<String>,
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
    /// See [`RuntimeUpgradeRecord::attempt_id`]; `None` for an upgrade
    /// begun with attempt finality off.
    pub attempt_id: Option<&'a str>,
}

/// A fresh attempt id, for an `UpgradeRuntime` request that named none: the
/// node generates it and records it with the attempt. A UUIDv7: time-ordered
/// with random bits, so ids the node generates don't collide in practice.
pub fn new_runtime_upgrade_attempt_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

#[derive(Clone, Copy, Debug)]
pub struct ObservedSandbox<'a> {
    pub state: SandboxStatus,
    pub current_local_artifact_id: Option<LocalArtifactId>,
    pub execution: Option<&'a ExecutionMetadata>,
    /// The activation the row last recorded, which every start replaces, so
    /// a start that ran and stopped again in between doesn't read as the
    /// same row even when the other columns are back as they were. Only the
    /// recorders of a refusal or an already-on-target outcome compare it;
    /// `begin_runtime_upgrade` doesn't need to, since the fence preserves
    /// the disk as it is when it fences, whatever ran before.
    pub node_execution_id: Option<SandboxActivationAttemptId>,
}

impl<'a> ObservedSandbox<'a> {
    pub fn of(sandbox: &'a SandboxRow) -> Self {
        Self {
            state: sandbox.state,
            current_local_artifact_id: sandbox.current_local_artifact_id,
            execution: sandbox.execution_metadata.as_ref(),
            node_execution_id: sandbox.node_execution_id,
        }
    }

    /// Whether `current` is still the row this observation saw, on its
    /// state, local artifact, execution metadata and last activation.
    fn matches(&self, current: &SandboxRow) -> bool {
        current.state == self.state
            && current.current_local_artifact_id == self.current_local_artifact_id
            && current.execution_metadata.as_ref() == self.execution
            && current.node_execution_id == self.node_execution_id
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum BeginRuntimeUpgrade {
    Fenced(Box<RuntimeUpgradeRecord>),
    SandboxMissing,
    /// Only a stopped or suspended sandbox has a disk/checkpoint pair to keep.
    UnsupportedState(SandboxStatus),
    /// A cleanup episode owns the sandbox.
    CleanupActive,
    /// Another upgrade fences the sandbox.
    OtherOperation(RuntimeUpgradeOperationId),
    /// A row with this operation id already exists.
    OperationExists,
    /// The node recorded this operation id's outcome for another sandbox, or
    /// a final attempt outcome for another request.
    OperationNamesAnother,
    /// A suspended sandbox's memory is lost, and the authorization didn't accept it.
    MemoryLossUnauthorized,
    /// The sandbox changed since the caller inspected it.
    SandboxChanged,
    /// The control plane abandoned this operation; see
    /// [`abandon_runtime_upgrade`].
    Abandoned,
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
    let attempt_id: Option<String> = row.get(14)?;
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
        attempt_id,
    })
}

/// The columns [`record_from_row`] decodes, in its order. Only ever the whole
/// `SELECT` list of a statement over `sandbox_runtime_upgrades` alone.
const RECORD_COLUMNS: &str = "operation_id, sandbox_id, target_environment, target_state, \
    authorization_id, authorization_reason, memory_loss_authorized, source_state, \
    previous_execution_metadata_json, checkpoint_artifact_id, checkpoint_dir_identity, \
    phase, failure, boot_execution_id, attempt_id";

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
    /// The upgrade that fences `sandbox_id`'s activations, if any: one in a
    /// phase of `FENCING_PHASES`.
    fn fencing_runtime_upgrade(
        conn: &Connection,
        sandbox_id: &str,
    ) -> Result<Option<(RuntimeUpgradeOperationId, RuntimeUpgradePhase)>> {
        static QUERY: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
            format!(
                "SELECT operation_id, phase
                   FROM sandbox_runtime_upgrades
                  WHERE sandbox_id = ?
                    AND phase IN ({})
                  ORDER BY created_at, operation_id
                  LIMIT 1",
                crate::db::sql_placeholders(FENCING_PHASES.len())
            )
        });
        let params = std::iter::once(sandbox_id)
            .chain(FENCING_PHASES.map(RuntimeUpgradePhase::as_str));
        let row: Option<(String, String)> = conn
            .query_row(QUERY.as_str(), rusqlite::params_from_iter(params), |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
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

/// The phases of an upgrade that still owns its sandbox, as an SQL list: a
/// fencing one, or a verified one whose retirement is pending. A suspended
/// eviction and its candidate scan hold off a sandbox in these too.
pub(crate) const UNRETIRED_UPGRADE_PHASES_SQL: &str =
    "('fenced', 'preserved', 'booting', 'verified')";

db_private_outside_tests! {
    /// The execution each of `sandbox_ids` publishes while a runtime upgrade
    /// fences it: the one the upgrade restores on a rollback
    /// ([`RuntimeUpgradeRecord::previous_execution`]). Sandboxes no upgrade
    /// fences are absent. The boot saves the target as the sandbox's
    /// consumer before it runs, and a failed boot's cleanup stops the
    /// sandbox with it still saved, so until the boot is verified the saved
    /// consumer isn't what the sandbox runs under; the rollback's restore
    /// commits with the end of the fence.
    fn fenced_runtime_upgrade_publications(
        conn: &Connection,
        sandbox_ids: &[String],
        budget: &mut pplx_space_rpc_contracts::inventory::InventoryBudget,
    ) -> std::result::Result<
        HashMap<String, Result<ExecutionMetadata>>,
        crate::db::inventory_read::InventoryReadError,
    > {
        if sandbox_ids.is_empty() {
            return Ok(HashMap::new());
        }
        // At most one upgrade fences a sandbox. Should that ever not hold,
        // the earliest wins, as it does in `fencing_runtime_upgrade`.
        let query = format!(
            "SELECT sandbox_id, previous_execution_metadata_json
               FROM sandbox_runtime_upgrades
              WHERE sandbox_id IN ({})
                AND phase IN ({})
              ORDER BY sandbox_id, created_at, operation_id",
            crate::db::sql_placeholders(sandbox_ids.len()),
            crate::db::sql_placeholders(FENCING_PHASES.len())
        );
        let params = sandbox_ids
            .iter()
            .map(String::as_str)
            .chain(FENCING_PHASES.map(RuntimeUpgradePhase::as_str));
        let mut statement = conn.prepare(&query)?;
        let mut rows = statement.query(rusqlite::params_from_iter(params))?;
        let mut publications = HashMap::new();
        while let Some(row) = rows.next()? {
            let sandbox_id: String = row.get(0)?;
            if publications.contains_key(&sandbox_id) {
                continue;
            }
            // Charged from SQLite's own buffer before it's copied or decoded,
            // as the State page charges each sandbox row it reads.
            let previous = row.get_ref(1)?;
            budget.charge_bytes(crate::db::inventory_read::value_raw_bytes(previous))?;
            // One that isn't text or doesn't decode is that sandbox's own
            // failure, which the State page publishes without an execution;
            // the rest of the page stands.
            let previous = previous
                .as_str()
                .map_err(|error| conversion_error(1, Type::Text, error))
                .and_then(|previous| {
                    serde_json::from_str::<ExecutionMetadata>(previous)
                        .map_err(|error| conversion_error(1, Type::Text, error))
                })
                .map_err(crate::db::Error::from);
            publications.insert(sandbox_id, previous);
        }
        Ok(publications)
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
                &format!(
                    "SELECT operation_id
                       FROM sandbox_runtime_upgrades
                      WHERE sandbox_id = ?1
                        AND phase IN {UNRETIRED_UPGRADE_PHASES_SQL}
                      ORDER BY created_at, operation_id
                      LIMIT 1"
                ),
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
    /// The upgrade of `sandbox_id` that must leave it `stopped` and hasn't
    /// stopped its boot yet, if any: a fencing one, whatever the sandbox's
    /// state, since its boot may be about to start and verify, or a verified
    /// one whose boot's execution still runs: the sandbox's current execution,
    /// on a started or paused row, as `boot_activity` in the service reads it.
    /// A stop keeps the execution's id, so the state decides. Once that
    /// execution is gone, by a stop or anything else, the upgrade never stops
    /// a later one, so only this window counts. A suspend that holds the
    /// artifact lock, which a fence takes, sees every upgrade that could boot
    /// before its capture.
    fn stay_stopped_runtime_upgrade_awaiting_its_stop(
        conn: &Connection,
        sandbox_id: &str,
    ) -> Result<Option<RuntimeUpgradeOperationId>> {
        let row: Option<String> = conn
            .query_row(
                "SELECT upgrade.operation_id
                   FROM sandbox_runtime_upgrades AS upgrade
                   JOIN sandboxes AS sandbox ON sandbox.id = upgrade.sandbox_id
                  WHERE upgrade.sandbox_id = ?1
                    AND upgrade.target_state = ?2
                    AND (upgrade.phase IN ('fenced', 'preserved', 'booting')
                         OR (upgrade.phase = 'verified'
                             AND sandbox.unowned_state IN ('started', 'paused')
                             AND upgrade.boot_execution_id = sandbox.node_execution_id))
                  ORDER BY upgrade.created_at, upgrade.operation_id
                  LIMIT 1",
                params![sandbox_id, RuntimeUpgradeTargetState::Stopped.as_str()],
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
    if runtime_upgrade_abandoned(&tx, new.operation_id)? {
        return Ok(BeginRuntimeUpgrade::Abandoned);
    }
    // An operation id names one upgrade of one sandbox, across both tables,
    // and asks for what its final attempt outcomes recorded, which outlive
    // the sandbox's row.
    let identity = RuntimeUpgradeRequestIdentity {
        operation_id: new.operation_id,
        sandbox_id: new.sandbox_id.to_owned(),
        target_environment: new.target_environment,
        target_state: new.target_state,
        authorization_id: new.authorization.id.clone(),
    };
    if recorded_runtime_upgrade_request(&tx, new.operation_id)?
        .is_some_and(|recorded| recorded.sandbox_id != new.sandbox_id)
        || retained_request_differs(&tx, &identity)?
    {
        return Ok(BeginRuntimeUpgrade::OperationNamesAnother);
    }
    let Some(sandbox) = get_sandbox(&tx, new.sandbox_id)? else {
        return Ok(BeginRuntimeUpgrade::SandboxMissing);
    };
    if !matches!(
        sandbox.state.unowned(),
        Some(SandboxState::Stopped | SandboxState::Suspended)
    ) {
        return Ok(BeginRuntimeUpgrade::UnsupportedState(sandbox.state));
    }
    if sandbox.state != new.observed.state
        || sandbox.current_local_artifact_id != new.observed.current_local_artifact_id
        || sandbox.execution_metadata.as_ref() != new.observed.execution
    {
        return Ok(BeginRuntimeUpgrade::SandboxChanged);
    }
    if sandbox.state.is_unowned(SandboxState::Suspended) && !new.authorization.memory_loss {
        return Ok(BeginRuntimeUpgrade::MemoryLossUnauthorized);
    }
    if cleanup_owns(&tx, &sandbox)? {
        return Ok(BeginRuntimeUpgrade::CleanupActive);
    }
    if let Some((other, _)) = fencing_runtime_upgrade(&tx, new.sandbox_id)? {
        return Ok(BeginRuntimeUpgrade::OtherOperation(other));
    }
    let checkpoint_artifact_id = (sandbox.state.is_unowned(SandboxState::Suspended)
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
             checkpoint_dir_identity, phase, failure, created_at, updated_at, attempt_id
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'fenced', NULL, ?12, ?12, ?13)",
        params![
            new.operation_id.to_string(),
            new.sandbox_id,
            new.target_environment.to_string(),
            new.target_state.as_str(),
            new.authorization.id,
            new.authorization.reason,
            new.authorization.memory_loss,
            sandbox.state.lifecycle().as_str(),
            previous_execution,
            checkpoint_artifact_id.map(|id| id.to_string()),
            new.checkpoint_dir_identity,
            now,
            new.attempt_id,
        ],
    )?;
    let record = runtime_upgrade(&tx, new.operation_id)?.ok_or_else(Error::no_rows)?;
    tx.commit()?;
    Ok(BeginRuntimeUpgrade::Fenced(Box::new(record)))
}

/// What [`abandon_runtime_upgrade`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AbandonRuntimeUpgrade {
    /// The operation is recorded abandoned, now or by an earlier call: no
    /// later call for it fences or boots.
    Abandoned,
    /// The operation hasn't settled: it fences the sandbox, so its boot may
    /// still change the disk, or its boot was verified and still owes the
    /// retirement, and for a stopped target the stop, a retry of it finishes.
    Unsettled,
    /// The node recorded this operation id for another sandbox.
    OperationNamesAnother,
}

/// Record `operation_id` abandoned for `sandbox_id`, so no later call for it
/// fences or boots, from any pod; see [`AbandonRuntimeUpgrade`]. An operation
/// the node never saw is recorded too, as is one for a sandbox the node
/// doesn't hold: its call may still be on its way, and one carrying
/// `stage_from` would stage the sandbox. The record outlives the sandbox's
/// row.
pub fn abandon_runtime_upgrade(
    conn: &Connection,
    sandbox_id: &str,
    operation_id: RuntimeUpgradeOperationId,
) -> Result<AbandonRuntimeUpgrade> {
    let tx = immediate_transaction(conn)?;
    let live = runtime_upgrade(&tx, operation_id)?;
    let outcome = if live
        .as_ref()
        .is_some_and(|record| record.sandbox_id != sandbox_id)
        || recorded_runtime_upgrade_request(&tx, operation_id)?
            .is_some_and(|recorded| recorded.sandbox_id != sandbox_id)
        || abandoned_runtime_upgrade_sandbox(&tx, operation_id)?
            .is_some_and(|abandoned| abandoned != sandbox_id)
        || attempt_outcome_names_another_sandbox(&tx, operation_id, sandbox_id)?
    {
        AbandonRuntimeUpgrade::OperationNamesAnother
    } else if live
        .as_ref()
        .is_some_and(|record| !record.phase.is_done_with_the_sandbox())
    {
        AbandonRuntimeUpgrade::Unsettled
    } else {
        tx.execute(
            "INSERT OR IGNORE INTO sandbox_runtime_upgrade_abandoned
                 (operation_id, sandbox_id, recorded_at)
             VALUES (?1, ?2, ?3)",
            params![operation_id.to_string(), sandbox_id, now()?],
        )?;
        AbandonRuntimeUpgrade::Abandoned
    };
    tx.commit()?;
    Ok(outcome)
}

/// Whether an attempt's final outcome records `operation_id` for another
/// sandbox than `sandbox_id`. Attempt outcomes outlive the sandbox's row, and
/// with it the operation's own outcome, so they still name the operation's
/// sandbox after that row is gone. A row without the request, which a build
/// from before the request columns writes, is final for no attempt, and
/// [`begin_runtime_upgrade`] doesn't hold an operation to it either: counting
/// it here would refuse to abandon an operation that same check let fence.
fn attempt_outcome_names_another_sandbox(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    sandbox_id: &str,
) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS (
             SELECT 1
               FROM sandbox_runtime_upgrade_attempt_outcomes
              WHERE operation_id = ?1
                AND sandbox_id != ?2
                AND target_environment IS NOT NULL
         )",
        params![operation_id.to_string(), sandbox_id],
        |row| row.get(0),
    )
}

db_private_outside_tests! {
    /// Whether the control plane abandoned `operation_id` on this node.
    fn runtime_upgrade_abandoned(
        conn: &Connection,
        operation_id: RuntimeUpgradeOperationId,
    ) -> Result<bool> {
        Ok(abandoned_runtime_upgrade_sandbox(conn, operation_id)?.is_some())
    }
}

/// The sandbox `operation_id` was abandoned for, if the control plane
/// abandoned it on this node.
pub(in crate::db) fn abandoned_runtime_upgrade_sandbox(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
) -> Result<Option<String>> {
    conn.query_row(
        "SELECT sandbox_id FROM sandbox_runtime_upgrade_abandoned WHERE operation_id = ?1",
        params![operation_id.to_string()],
        |row| row.get(0),
    )
    .optional()
}

/// Move `sandbox_id`'s upgrade `operation_id` one forward edge. A false
/// result means the row is gone, another writer moved it first, or the
/// operation's row is now another sandbox's: an operation id reused after
/// the row was deleted, which only that sandbox's driver may move.
pub fn advance_runtime_upgrade_for_sandbox(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    sandbox_id: &str,
    from: RuntimeUpgradePhase,
    to: RuntimeUpgradePhase,
) -> Result<bool> {
    advance_runtime_upgrade_in(conn, operation_id, Some(sandbox_id), from, to)
}

/// [`advance_runtime_upgrade_for_sandbox`], whichever sandbox's row the
/// operation's is.
///
/// Test-only: production code moves a row only for its own sandbox.
#[cfg(any(test, feature = "test-support"))]
pub fn advance_runtime_upgrade(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    from: RuntimeUpgradePhase,
    to: RuntimeUpgradePhase,
) -> Result<bool> {
    advance_runtime_upgrade_in(conn, operation_id, None, from, to)
}

fn advance_runtime_upgrade_in(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    sandbox_id: Option<&str>,
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
                AND phase = ?2
                AND (?5 IS NULL OR sandbox_id = ?5)",
            params![
                operation_id.to_string(),
                from.as_str(),
                to.as_str(),
                now()?,
                sandbox_id
            ],
        )?
    } else {
        conn.execute(
            "UPDATE sandbox_runtime_upgrades
                SET phase = ?3, updated_at = ?4
              WHERE operation_id = ?1
                AND phase = ?2
                AND (?5 IS NULL OR sandbox_id = ?5)",
            params![
                operation_id.to_string(),
                from.as_str(),
                to.as_str(),
                now()?,
                sandbox_id
            ],
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
    sandbox_id: &str,
    consumer: &ExecutionMetadata,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let Some(record) = runtime_upgrade(&tx, operation_id)? else {
        return Ok(false);
    };
    if record.sandbox_id != sandbox_id
        || record.phase != RuntimeUpgradePhase::Preserved
        || consumer.environment() != Some(record.target_environment)
    {
        return Ok(false);
    }
    upsert_sandbox_execution_metadata_in_transaction(&tx, &record.sandbox_id, consumer)?;
    let advanced = advance_runtime_upgrade_in(
        &tx,
        operation_id,
        Some(&record.sandbox_id),
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
/// With `attempt_finality`, the same transaction records the rollback as the
/// final outcome of the attempt the row runs as (see
/// [`read::runtime_upgrade_attempt_outcome`]), so it's final for that attempt
/// from the moment the operation's report names it, whether or not the
/// preserved disk is ever discarded. An attempt that already has a final
/// outcome keeps it. Without `attempt_finality`, or for a row written before
/// attempts were recorded, no attempt's outcome is recorded. False, changing
/// nothing, when the operation's row is now another sandbox's than
/// `sandbox_id`: an operation id reused after the row was deleted, which only
/// that sandbox's driver may settle.
pub fn roll_back_runtime_upgrade_for_sandbox(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    sandbox_id: &str,
    failure: &str,
    attempt_finality: bool,
) -> Result<bool> {
    roll_back_runtime_upgrade_in(
        conn,
        operation_id,
        Some(sandbox_id),
        failure,
        attempt_finality,
    )
}

/// [`roll_back_runtime_upgrade_for_sandbox`], whichever sandbox's row the
/// operation's is.
///
/// Test-only: production code moves a row only for its own sandbox.
#[cfg(any(test, feature = "test-support"))]
pub fn roll_back_runtime_upgrade(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    failure: &str,
    attempt_finality: bool,
) -> Result<bool> {
    roll_back_runtime_upgrade_in(conn, operation_id, None, failure, attempt_finality)
}

fn roll_back_runtime_upgrade_in(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    sandbox_id: Option<&str>,
    failure: &str,
    attempt_finality: bool,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let Some(record) = runtime_upgrade(&tx, operation_id)? else {
        return Ok(false);
    };
    if !record.phase.fences_activation()
        || sandbox_id.is_some_and(|sandbox_id| sandbox_id != record.sandbox_id)
    {
        return Ok(false);
    }
    upsert_sandbox_execution_metadata_in_transaction(
        &tx,
        &record.sandbox_id,
        &record.previous_execution,
    )?;
    // One time for the row and its attempt's outcome: the report names the
    // attempt only while the two agree on it.
    let rolled_back_at = now()?;
    let changed = tx.execute(
        "UPDATE sandbox_runtime_upgrades
            SET phase = 'rolled_back', failure = ?3, updated_at = ?4
          WHERE operation_id = ?1
            AND phase = ?2
            AND sandbox_id = ?5",
        params![
            operation_id.to_string(),
            record.phase.as_str(),
            failure,
            rolled_back_at,
            record.sandbox_id
        ],
    )?;
    if changed != 1 {
        return Ok(false);
    }
    if attempt_finality && let Some(attempt_id) = &record.attempt_id {
        record_final_attempt_outcome(
            &tx,
            &record.identity(),
            attempt_id,
            RecordedAttemptOutcome::RolledBack,
            Some(failure),
            &rolled_back_at,
        )?;
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
/// failure and its attempt as the operation's
/// [`RecordedRuntimeUpgradeOutcome::RolledBack`] outcome, at the time the
/// rollback was recorded. The attempt's own final outcome, when attempt
/// finality recorded one, was written with the rollback
/// ([`roll_back_runtime_upgrade_for_sandbox`]) and stays. An attempt whose
/// final outcome is another one keeps it, and the operation's outcome is left
/// as it stands, so the two never disagree about that attempt; the row is
/// deleted either way.
/// False, changing nothing, when the operation's row is now another
/// sandbox's than `sandbox_id`.
pub fn finish_runtime_upgrade_rollback_for_sandbox(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    sandbox_id: &str,
) -> Result<bool> {
    finish_runtime_upgrade_rollback_in(conn, operation_id, Some(sandbox_id))
}

/// [`finish_runtime_upgrade_rollback_for_sandbox`], whichever sandbox's
/// row the operation's is.
///
/// Test-only: production code moves a row only for its own sandbox.
#[cfg(any(test, feature = "test-support"))]
pub fn finish_runtime_upgrade_rollback(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
) -> Result<bool> {
    finish_runtime_upgrade_rollback_in(conn, operation_id, None)
}

fn finish_runtime_upgrade_rollback_in(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    sandbox_id: Option<&str>,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    // The rollback's own time, which its attempt's outcome carries: the
    // report trusts the outcome's attempt only while the two agree.
    tx.execute(
        "INSERT INTO sandbox_runtime_upgrade_outcomes (
             operation_id, sandbox_id, outcome, target_environment, target_state,
             authorization_id, failure, recorded_at, attempt_id
         )
         SELECT operation_id, sandbox_id, ?2, target_environment, target_state,
                authorization_id, failure, updated_at, attempt_id
           FROM sandbox_runtime_upgrades AS upgrade
          WHERE operation_id = ?1
            AND phase = 'rolled_back'
            AND (?4 IS NULL OR sandbox_id = ?4)
            AND NOT EXISTS (
                SELECT 1
                  FROM sandbox_runtime_upgrade_attempt_outcomes AS attempt
                 WHERE attempt.operation_id = upgrade.operation_id
                   AND attempt.attempt_id = upgrade.attempt_id
                   AND attempt.target_environment IS NOT NULL
                   AND NOT (
                       attempt.outcome = ?3
                       AND attempt.failure IS upgrade.failure
                       AND attempt.recorded_at = upgrade.updated_at
                   )
            )
         ON CONFLICT (operation_id) DO UPDATE
            SET outcome = excluded.outcome,
                failure = excluded.failure,
                recorded_at = excluded.recorded_at,
                attempt_id = excluded.attempt_id
          WHERE sandbox_id = excluded.sandbox_id",
        params![
            operation_id.to_string(),
            RecordedRuntimeUpgradeOutcome::RolledBack.as_str(),
            RecordedAttemptOutcome::RolledBack.as_str(),
            sandbox_id,
        ],
    )?;
    let changed = tx.execute(
        "DELETE FROM sandbox_runtime_upgrades
          WHERE operation_id = ?1
            AND phase = 'rolled_back'
            AND (?2 IS NULL OR sandbox_id = ?2)",
        params![operation_id.to_string(), sandbox_id],
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

/// Record `outcome` as the final outcome of the attempt `attempt_id` of the
/// operation `request` names, with what it asked for, inside the caller's
/// transaction. False, recording nothing, when the attempt already has a
/// final outcome. A row without the request, which a build from before the
/// request columns writes when a rollback finishes, is final for no attempt,
/// and this one replaces it, with a new `recorded_seq`, since it's recorded
/// now.
fn record_final_attempt_outcome(
    tx: &Connection,
    request: &RuntimeUpgradeRequestIdentity,
    attempt_id: &str,
    outcome: RecordedAttemptOutcome,
    failure: Option<&str>,
    recorded_at: &str,
) -> Result<bool> {
    tx.execute(
        "DELETE FROM sandbox_runtime_upgrade_attempt_outcomes
          WHERE operation_id = ?1
            AND attempt_id = ?2
            AND target_environment IS NULL",
        params![request.operation_id.to_string(), attempt_id],
    )?;
    let changed = tx.execute(
        "INSERT INTO sandbox_runtime_upgrade_attempt_outcomes (
             operation_id, attempt_id, sandbox_id, outcome, failure, recorded_at,
             target_environment, target_state, authorization_id
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT (operation_id, attempt_id) DO NOTHING",
        params![
            request.operation_id.to_string(),
            attempt_id,
            request.sandbox_id,
            outcome.as_str(),
            failure,
            recorded_at,
            request.target_environment.to_string(),
            request.target_state.as_str(),
            request.authorization_id,
        ],
    )?;
    Ok(changed == 1)
}

/// Whether a final attempt outcome of `request`'s operation records another
/// request: another sandbox, target, target state or authorization. Those
/// rows outlive the sandbox's row, so they keep the operation one request
/// after its live row and its outcome are gone.
fn retained_request_differs(
    conn: &Connection,
    request: &RuntimeUpgradeRequestIdentity,
) -> Result<bool> {
    Ok(
        retained_runtime_upgrade_requests(conn, request.operation_id)?
            .iter()
            .any(|retained| retained != request),
    )
}

db_private_outside_tests! {
    /// What the operation `operation_id` asked for, as each of its attempts'
    /// final outcomes recorded it, one entry per distinct request. Empty
    /// without one. These outlive the sandbox's row, and the operation's
    /// report with it.
    fn retained_runtime_upgrade_requests(
        conn: &Connection,
        operation_id: RuntimeUpgradeOperationId,
    ) -> Result<Vec<RuntimeUpgradeRequestIdentity>> {
        let mut statement = conn.prepare(
            "SELECT DISTINCT sandbox_id, target_environment, target_state, authorization_id
               FROM sandbox_runtime_upgrade_attempt_outcomes
              WHERE operation_id = ?1
                AND target_environment IS NOT NULL",
        )?;
        let rows = statement.query_map(params![operation_id.to_string()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?;
        rows.map(|row| {
            let (sandbox_id, target_environment, target_state, authorization_id) = row?;
            Ok(RuntimeUpgradeRequestIdentity {
                operation_id,
                sandbox_id,
                target_environment: target_environment
                    .parse()
                    .map_err(|error| conversion_error(1, Type::Text, error))?,
                target_state: target_state
                    .parse()
                    .map_err(|error| conversion_error(2, Type::Text, error))?,
                authorization_id: authorization_id.ok_or_else(|| {
                    conversion_error(3, Type::Text, MissingRetainedAuthorization)
                })?,
            })
        })
        .collect()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("a final runtime upgrade attempt outcome names no authorization")]
struct MissingRetainedAuthorization;

/// Record that the operation `request` names found its sandbox already on
/// its target and fenced nothing, as the attempt `attempt_id`, or as no
/// attempt (`None`) when it was decided with attempt finality off, so the
/// outcome never names an attempt the switch said nothing about, whatever the
/// switch is when it's read. The outcome keeps naming the attempt, or no
/// attempt, that first found the target: a replay, under any attempt id,
/// leaves it. False, recording nothing, when the attempt already
/// has a final outcome of its own, or the node already recorded the
/// operation, as an outcome or a live upgrade, for another sandbox, or as an
/// outcome or a final attempt outcome for another request, or the control
/// plane abandoned it here (see [`abandon_runtime_upgrade`]).
/// The checks and the write are one transaction, and `begin_runtime_upgrade`
/// checks this table in its transaction, so two requests reusing one
/// operation id can't both succeed.
pub fn record_runtime_upgrade_already_on_target(
    conn: &Connection,
    request: &RuntimeUpgradeRequestIdentity,
    attempt_id: Option<&str>,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    if !insert_already_on_target(&tx, request, attempt_id)? {
        return Ok(false);
    }
    tx.commit()?;
    Ok(true)
}

/// What [`record_runtime_upgrade_already_on_target_observed`] did.
#[derive(Debug, Eq, PartialEq)]
pub enum AlreadyOnTargetRecord {
    Recorded,
    /// What [`record_runtime_upgrade_already_on_target`] answers false for.
    NotRecorded,
    /// The sandbox is gone, no longer as `observed` saw it, or a cleanup owns
    /// it (`cleanup_owns`): the target it was found on may be a copy the
    /// cleanup is removing, so nothing is recorded and a retry decides again.
    SandboxMoved,
}

/// [`record_runtime_upgrade_already_on_target`], decided from the sandbox's
/// row as `observed` saw it: recorded only while the row is still as it was,
/// on its state, local artifact, execution metadata and last activation, and
/// no cleanup owns
/// the sandbox. The checks and the write are one transaction.
pub fn record_runtime_upgrade_already_on_target_observed(
    conn: &Connection,
    request: &RuntimeUpgradeRequestIdentity,
    attempt_id: Option<&str>,
    observed: ObservedSandbox<'_>,
) -> Result<AlreadyOnTargetRecord> {
    let tx = immediate_transaction(conn)?;
    let Some(current) = get_sandbox(&tx, &request.sandbox_id)? else {
        return Ok(AlreadyOnTargetRecord::SandboxMoved);
    };
    if cleanup_owns(&tx, &current)? || !observed.matches(&current) {
        return Ok(AlreadyOnTargetRecord::SandboxMoved);
    }
    if !insert_already_on_target(&tx, request, attempt_id)? {
        return Ok(AlreadyOnTargetRecord::NotRecorded);
    }
    tx.commit()?;
    Ok(AlreadyOnTargetRecord::Recorded)
}

/// The checks and the write of [`record_runtime_upgrade_already_on_target`],
/// in the caller's transaction, which commits only on true.
fn insert_already_on_target(
    tx: &Connection,
    request: &RuntimeUpgradeRequestIdentity,
    attempt_id: Option<&str>,
) -> Result<bool> {
    if retained_request_differs(tx, request)? {
        return Ok(false);
    }
    // `attempt_id = NULL` matches no row, so an attempt-less outcome skips
    // the attempt's own final outcome here; its caller checked that first.
    let changed = tx.execute(
        "INSERT INTO sandbox_runtime_upgrade_outcomes (
             operation_id, sandbox_id, outcome, target_environment, target_state,
             authorization_id, failure, recorded_at, attempt_id
         )
         SELECT ?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, ?8
          WHERE NOT EXISTS (
                SELECT 1
                  FROM sandbox_runtime_upgrades
                 WHERE operation_id = ?1
                   AND sandbox_id != ?2
          )
            AND NOT EXISTS (
                SELECT 1
                  FROM sandbox_runtime_upgrade_abandoned
                 WHERE operation_id = ?1
          )
            AND NOT EXISTS (
                SELECT 1
                  FROM sandbox_runtime_upgrade_attempt_outcomes
                 WHERE operation_id = ?1
                   AND attempt_id = ?8
                   AND target_environment IS NOT NULL
          )
         ON CONFLICT (operation_id) DO UPDATE
            SET outcome = excluded.outcome,
                failure = NULL,
                recorded_at = excluded.recorded_at,
                attempt_id = CASE
                    WHEN outcome = excluded.outcome THEN attempt_id
                    ELSE excluded.attempt_id
                END
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
            now()?,
            attempt_id,
        ],
    )?;
    Ok(changed == 1)
}

/// Whether a cleanup owns `sandbox`: its row carries a cleanup intent or a
/// cleanup-owned state, or a cleanup episode is outstanding. Meanwhile the
/// sandbox's disk or checkpoint may be a copy the cleanup is removing, so no
/// refusal decided from them is final for an attempt.
fn cleanup_owns(conn: &Connection, sandbox: &SandboxRow) -> Result<bool> {
    Ok(sandbox.cleanup_completion.is_some()
        || sandbox.state.is_cleanup_owned()
        || cleanup_episode(conn, &sandbox.id)?.is_some())
}

/// Record that the operation `request` names was refused for the attempt
/// `attempt_id` before it fenced anything, with `failure` as the reason, so the
/// refusal is final for that attempt as a rollback is: it becomes the
/// operation's outcome and the attempt's own (see
/// [`read::runtime_upgrade_attempt_outcome`]). The caller records only a refusal a
/// retry can't change. False, recording nothing, when the attempt already has
/// a final outcome of its own, the sandbox is gone from the node, a live
/// upgrade row exists for the operation, the control plane abandoned it here,
/// or its recorded outcome or a final attempt outcome names another request.
/// An already-on-target outcome isn't replaced: it keeps the operation from
/// ever booting, and its activation claim with it, so beside one only the
/// attempt's own outcome records the refusal, and that attempt's report and
/// retries answer with it. False too when the refusal was decided from the
/// sandbox as `decided_on` saw it and the sandbox's row no longer matches it
/// on its state, local artifact, execution metadata and last activation: what
/// the refusal said of the sandbox may no longer hold, and a retry decides it
/// again. `None` for a refusal that doesn't depend on the sandbox's row.
/// False too while a cleanup owns the sandbox (`cleanup_owns`), including
/// one admitted while the refusal was decided. The checks and the write are
/// one transaction.
pub fn record_runtime_upgrade_refusal(
    conn: &Connection,
    request: &RuntimeUpgradeRequestIdentity,
    attempt_id: &str,
    failure: &str,
    decided_on: Option<ObservedSandbox<'_>>,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    if let Some(current) = get_sandbox(&tx, &request.sandbox_id)?
        && cleanup_owns(&tx, &current)?
    {
        return Ok(false);
    }
    if let Some(decided) = decided_on {
        let unchanged =
            get_sandbox(&tx, &request.sandbox_id)?.is_some_and(|current| decided.matches(&current));
        if !unchanged {
            return Ok(false);
        }
    }
    if retained_request_differs(&tx, request)? {
        return Ok(false);
    }
    let recorded_at = now()?;
    let changed = tx.execute(
        "INSERT INTO sandbox_runtime_upgrade_outcomes (
             operation_id, sandbox_id, outcome, target_environment, target_state,
             authorization_id, failure, recorded_at, attempt_id
         )
         SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9
          WHERE EXISTS (SELECT 1 FROM sandboxes WHERE id = ?2)
            AND NOT EXISTS (
                SELECT 1
                  FROM sandbox_runtime_upgrades
                 WHERE operation_id = ?1
          )
            AND NOT EXISTS (
                SELECT 1
                  FROM sandbox_runtime_upgrade_abandoned
                 WHERE operation_id = ?1
          )
            AND NOT EXISTS (
                SELECT 1
                  FROM sandbox_runtime_upgrade_attempt_outcomes
                 WHERE operation_id = ?1
                   AND attempt_id = ?9
                   AND target_environment IS NOT NULL
          )
         ON CONFLICT (operation_id) DO UPDATE
            SET outcome = excluded.outcome,
                failure = excluded.failure,
                recorded_at = excluded.recorded_at,
                attempt_id = excluded.attempt_id
          WHERE sandbox_id = excluded.sandbox_id
            AND target_environment = excluded.target_environment
            AND target_state = excluded.target_state
            AND authorization_id = excluded.authorization_id
            AND outcome IN (?3, ?10)",
        params![
            request.operation_id.to_string(),
            request.sandbox_id,
            RecordedRuntimeUpgradeOutcome::Refused.as_str(),
            request.target_environment.to_string(),
            request.target_state.as_str(),
            request.authorization_id,
            failure,
            recorded_at,
            attempt_id,
            RecordedRuntimeUpgradeOutcome::RolledBack.as_str(),
        ],
    )?;
    if changed != 1 && !already_on_target_for(&tx, request, attempt_id)? {
        return Ok(false);
    }
    // Beside an already-on-target outcome the operation keeps it, and only
    // the attempt's own outcome records the refusal.
    if !record_final_attempt_outcome(
        &tx,
        request,
        attempt_id,
        RecordedAttemptOutcome::Refused,
        Some(failure),
        &recorded_at,
    )? {
        return Ok(false);
    }
    tx.commit()?;
    Ok(true)
}

/// Whether the operation `request` names recorded an already-on-target
/// outcome for the same request under another attempt than `attempt_id`,
/// with no live upgrade row, its sandbox still on the node and the operation
/// not abandoned here: an earlier attempt found the target, so a later
/// attempt's refusal is that attempt's alone. The attempt that found the target keeps that answer.
fn already_on_target_for(
    tx: &Connection,
    request: &RuntimeUpgradeRequestIdentity,
    attempt_id: &str,
) -> Result<bool> {
    Ok(tx
        .query_row(
            "SELECT 1
               FROM sandbox_runtime_upgrade_outcomes
              WHERE operation_id = ?1
                AND sandbox_id = ?2
                AND target_environment = ?3
                AND target_state = ?4
                AND authorization_id = ?5
                AND outcome = ?6
                AND attempt_id IS NOT ?7
                AND EXISTS (SELECT 1 FROM sandboxes WHERE id = ?2)
                AND NOT EXISTS (
                    SELECT 1 FROM sandbox_runtime_upgrades WHERE operation_id = ?1
                )
                AND NOT EXISTS (
                    SELECT 1 FROM sandbox_runtime_upgrade_abandoned WHERE operation_id = ?1
                )",
            params![
                request.operation_id.to_string(),
                request.sandbox_id,
                request.target_environment.to_string(),
                request.target_state.as_str(),
                request.authorization_id,
                RecordedRuntimeUpgradeOutcome::AlreadyOnTarget.as_str(),
                attempt_id,
            ],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

/// The final outcome the operation recorded for the attempt `attempt_id`:
/// its rollback ([`roll_back_runtime_upgrade_for_sandbox`]), its refusal
/// before the fence ([`record_runtime_upgrade_refusal`]), or its refusal
/// after the operation reached its target
/// ([`record_runtime_upgrade_refused_attempt`]). An attempt has at most one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeUpgradeAttemptOutcome {
    pub outcome: RecordedAttemptOutcome,
    pub failure: Option<String>,
}

/// What one attempt ended in, in `sandbox_runtime_upgrade_attempt_outcomes`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordedAttemptOutcome {
    /// The attempt's upgrade was rolled back.
    RolledBack,
    /// The attempt was refused before it fenced anything, for a reason no
    /// retry changes.
    Refused,
    /// The attempt was refused after the operation reached its target, so the
    /// sandbox doesn't run for it.
    ActivationRefused,
}

impl RecordedAttemptOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RolledBack => "rolled_back",
            Self::Refused => "refused",
            Self::ActivationRefused => "activation_refused",
        }
    }
}

impl FromStr for RecordedAttemptOutcome {
    type Err = RecordedRuntimeUpgradeOutcomeParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "rolled_back" => Ok(Self::RolledBack),
            "refused" => Ok(Self::Refused),
            "activation_refused" => Ok(Self::ActivationRefused),
            other => Err(RecordedRuntimeUpgradeOutcomeParseError(other.to_string())),
        }
    }
}

db_private_outside_tests! {
    /// The final outcome of the attempt `attempt_id` under `operation_id`,
    /// `None` when none was recorded for it. Unlike the operation's report,
    /// which keeps only its latest outcome, this is the attempt's own,
    /// whatever later attempts recorded since. A row without the request it
    /// was recorded for is final for no attempt and isn't read.
    fn runtime_upgrade_attempt_outcome(
        conn: &Connection,
        operation_id: RuntimeUpgradeOperationId,
        attempt_id: &str,
    ) -> Result<Option<RuntimeUpgradeAttemptOutcome>> {
        let row: Option<(String, Option<String>)> = conn
            .query_row(
                "SELECT outcome, failure
                   FROM sandbox_runtime_upgrade_attempt_outcomes
                  WHERE operation_id = ?1
                    AND attempt_id = ?2
                    AND target_environment IS NOT NULL",
                params![operation_id.to_string(), attempt_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        row.map(|(outcome, failure)| {
            Ok(RuntimeUpgradeAttemptOutcome {
                outcome: outcome
                    .parse()
                    .map_err(|error| conversion_error(0, Type::Text, error))?,
                failure,
            })
        })
        .transpose()
    }
}

db_private_outside_tests! {
    /// [`runtime_upgrade_attempt_outcome`], with the request the outcome was
    /// recorded for, read from the same row: an operation id reused for
    /// another sandbox's request can record an outcome under the same
    /// attempt id, and that one isn't this request's to replay.
    fn runtime_upgrade_attempt_outcome_with_request(
        conn: &Connection,
        operation_id: RuntimeUpgradeOperationId,
        attempt_id: &str,
    ) -> Result<Option<(RuntimeUpgradeAttemptOutcome, RuntimeUpgradeRequestIdentity)>> {
        type Row = (String, Option<String>, String, String, String, Option<String>);
        let row: Option<Row> = conn
            .query_row(
                "SELECT outcome, failure, sandbox_id, target_environment, target_state,
                        authorization_id
                   FROM sandbox_runtime_upgrade_attempt_outcomes
                  WHERE operation_id = ?1
                    AND attempt_id = ?2
                    AND target_environment IS NOT NULL",
                params![operation_id.to_string(), attempt_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;
        row.map(
            |(outcome, failure, sandbox_id, target_environment, target_state, authorization_id)| {
                Ok((
                    RuntimeUpgradeAttemptOutcome {
                        outcome: outcome
                            .parse()
                            .map_err(|error| conversion_error(0, Type::Text, error))?,
                        failure,
                    },
                    RuntimeUpgradeRequestIdentity {
                        operation_id,
                        sandbox_id,
                        target_environment: target_environment
                            .parse()
                            .map_err(|error| conversion_error(3, Type::Text, error))?,
                        target_state: target_state
                            .parse()
                            .map_err(|error| conversion_error(4, Type::Text, error))?,
                        authorization_id: authorization_id.ok_or_else(|| {
                            conversion_error(5, Type::Text, MissingRetainedAuthorization)
                        })?,
                    },
                ))
            },
        )
        .transpose()
    }
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

/// Record that the operation `operation_id` refused the attempt `attempt_id`
/// after it reached its target for `sandbox_id`: the node upgraded the
/// sandbox (the live row is retired), or found it there already (no live
/// row, an already-on-target outcome), but the attempt's `UpgradeRuntime` for
/// a started target failed, so the sandbox doesn't run for that attempt. The
/// refusal is final for that attempt, its
/// [`RecordedAttemptOutcome::ActivationRefused`] outcome; `failure` says why.
/// False, recording nothing, when the operation hasn't reached its target for
/// that sandbox: its rollback or refusal is what the attempt's report says
/// then, and when a final attempt outcome records the operation for another
/// request. False too, keeping the recorded one, when the attempt already has
/// a final outcome: a final row is never rewritten, so `recorded_seq` order
/// is recording order (see `latest_refused_attempt`). The attempt's outcome
/// keeps the request of the record that reached the target. False too when the
/// sandbox's row isn't `decided_on`, the row the caller decided the refusal
/// was final from (`None` when it found none): another start, stop or suspend
/// moved the sandbox since, so the refusal may not be the attempt's to keep,
/// and a retry decides it again. The row is compared on what that decision
/// reads: its runtime label, state, execution, route identity and pinned
/// artifact, which a start or backup can pin without the upgrade's lock, so a
/// checkpoint refusal can't outlive the pin it was decided from. False too
/// while a cleanup owns the sandbox (`cleanup_owns`). The checks and the
/// write are one transaction.
pub fn record_runtime_upgrade_refused_attempt(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    sandbox_id: &str,
    attempt_id: &str,
    failure: &str,
    decided_on: Option<&SandboxRow>,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let current = get_sandbox(&tx, sandbox_id)?;
    let unchanged = match (current.as_ref(), decided_on) {
        (None, None) => true,
        (Some(current), Some(decided)) => {
            !cleanup_owns(&tx, current)?
                && current.uuid8 == decided.uuid8
                && current.state == decided.state
                && current.node_execution_id == decided.node_execution_id
                && current.space_api_route_identity == decided.space_api_route_identity
                && current.current_local_artifact_id == decided.current_local_artifact_id
        }
        _ => false,
    };
    if !unchanged {
        return Ok(false);
    }
    // The request comes from the record that reached the target, so the
    // attempt's outcome keeps what its operation asked for.
    let reached: Option<(String, String, String)> = tx
        .query_row(
            "SELECT target_environment, target_state, authorization_id
               FROM sandbox_runtime_upgrades
              WHERE operation_id = ?1
                AND sandbox_id = ?2
                AND phase = 'retired'
             UNION ALL
             SELECT target_environment, target_state, authorization_id
               FROM sandbox_runtime_upgrade_outcomes
              WHERE operation_id = ?1
                AND sandbox_id = ?2
                AND outcome = ?3
                AND NOT EXISTS (
                    SELECT 1 FROM sandbox_runtime_upgrades WHERE operation_id = ?1
                )",
            params![
                operation_id.to_string(),
                sandbox_id,
                RecordedRuntimeUpgradeOutcome::AlreadyOnTarget.as_str(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((target_environment, target_state, authorization_id)) = reached else {
        return Ok(false);
    };
    let request = RuntimeUpgradeRequestIdentity {
        operation_id,
        sandbox_id: sandbox_id.to_owned(),
        target_environment: target_environment
            .parse()
            .map_err(|error| conversion_error(0, Type::Text, error))?,
        target_state: target_state
            .parse()
            .map_err(|error| conversion_error(1, Type::Text, error))?,
        authorization_id,
    };
    if retained_request_differs(&tx, &request)? {
        return Ok(false);
    }
    let recorded = record_final_attempt_outcome(
        &tx,
        &request,
        attempt_id,
        RecordedAttemptOutcome::ActivationRefused,
        Some(failure),
        &now()?,
    )?;
    tx.commit()?;
    Ok(recorded)
}

/// The latest attempt the operation refused after reaching its target,
/// `None` without one. A final row is never rewritten, and while the table
/// exists `recorded_seq` is never reused or renumbered, so the highest is the
/// latest recorded, however coarse or unsteady the clock that stamped
/// `recorded_at`. The order is all this reads: a down and up restarts the
/// sequence. A row kept from before its sandbox was deleted can be the
/// latest; a caller reads it only beside a report that reached the target.
fn latest_refused_attempt(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
) -> Result<Option<RuntimeUpgradeRefusedAttempt>> {
    conn.query_row(
        "SELECT attempt_id, failure
           FROM sandbox_runtime_upgrade_attempt_outcomes
          WHERE operation_id = ?1
            AND outcome = ?2
            AND target_environment IS NOT NULL
          ORDER BY recorded_seq DESC
          LIMIT 1",
        params![
            operation_id.to_string(),
            RecordedAttemptOutcome::ActivationRefused.as_str()
        ],
        refused_attempt_from_row,
    )
    .optional()
}

fn refused_attempt_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<RuntimeUpgradeRefusedAttempt> {
    Ok(RuntimeUpgradeRefusedAttempt {
        attempt_id: row.get(0)?,
        failure: row.get(1)?,
    })
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
    /// The request was refused before it fenced anything, for a reason no
    /// retry changes.
    Refused,
}

impl RecordedRuntimeUpgradeOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RolledBack => "rolled_back",
            Self::AlreadyOnTarget => "already_on_target",
            Self::Refused => "refused",
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
            "refused" => Ok(Self::Refused),
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
    /// The attempt the reported outcome ran as, as its live row or recorded
    /// outcome names it; `None` for a row written before the node recorded
    /// attempts, or for a recorded outcome a store without attempts rewrote
    /// since (see `recorded_outcome_attempt`).
    pub attempt_id: Option<String>,
    /// The latest attempt the operation refused after it reached its target,
    /// so the sandbox doesn't run for that attempt; see
    /// [`record_runtime_upgrade_refused_attempt`]. Only an upgraded or
    /// already-on-target operation refuses one, but the row outlives its
    /// sandbox's, so beside another status it was kept from before the
    /// sandbox was deleted and says nothing of this report: read it only
    /// beside an upgraded or already-on-target status.
    pub refused_attempt: Option<RuntimeUpgradeRefusedAttempt>,
}

/// An attempt an operation refused after it reached its target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeUpgradeRefusedAttempt {
    pub attempt_id: String,
    pub failure: Option<String>,
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
    /// The request was refused before it fenced anything; `failure` says why.
    Refused { failure: Option<String> },
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
    /// new attempt after a rollback or a refusal starts over under the same
    /// operation.
    fn runtime_upgrade_report(
        conn: &Connection,
        operation_id: RuntimeUpgradeOperationId,
    ) -> Result<Option<RuntimeUpgradeReport>> {
        if let Some(record) = runtime_upgrade(conn, operation_id)? {
            // A rolled-back row names its attempt only while the attempt's
            // final outcome, written in the rollback's transaction, agrees:
            // a rollback that recorded no final outcome, as one with
            // finality off does, is final for no attempt.
            let attempt_id = match (record.phase, record.attempt_id) {
                (RuntimeUpgradePhase::RolledBack, Some(attempt_id)) => {
                    let updated_at: String = conn.query_row(
                        "SELECT updated_at FROM sandbox_runtime_upgrades WHERE operation_id = ?1",
                        params![operation_id.to_string()],
                        |row| row.get(0),
                    )?;
                    recorded_outcome_attempt(
                        conn,
                        operation_id,
                        attempt_id,
                        RecordedRuntimeUpgradeOutcome::RolledBack,
                        record.failure.as_deref(),
                        &updated_at,
                    )?
                }
                (_, attempt_id) => attempt_id,
            };
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
                attempt_id,
                refused_attempt: latest_refused_attempt(conn, operation_id)?,
            }));
        }
        type OutcomeRow = (
            String,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
        );
        let row: Option<OutcomeRow> = conn
            .query_row(
                "SELECT sandbox_id, outcome, failure, activation_claim_kind,
                        activation_claimed_execution_id, attempt_id, recorded_at
                   FROM sandbox_runtime_upgrade_outcomes
                  WHERE operation_id = ?1",
                params![operation_id.to_string()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(sandbox_id, outcome, failure, claim_kind, claimed_execution, attempt_id, recorded_at)| {
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
            let outcome: RecordedRuntimeUpgradeOutcome = outcome
                .parse()
                .map_err(|error| conversion_error(1, Type::Text, error))?;
            let attempt_id = match attempt_id {
                Some(attempt_id) => recorded_outcome_attempt(
                    conn,
                    operation_id,
                    attempt_id,
                    outcome,
                    failure.as_deref(),
                    &recorded_at,
                )?,
                None => None,
            };
            let status = match outcome {
                RecordedRuntimeUpgradeOutcome::RolledBack => RuntimeUpgradeStatus::RolledBack { failure },
                RecordedRuntimeUpgradeOutcome::Refused => RuntimeUpgradeStatus::Refused { failure },
                RecordedRuntimeUpgradeOutcome::AlreadyOnTarget => {
                    RuntimeUpgradeStatus::AlreadyOnTarget { activation }
                }
            };
            Ok(RuntimeUpgradeReport {
                sandbox_id,
                status,
                attempt_id,
                refused_attempt: latest_refused_attempt(conn, operation_id)?,
            })
        })
        .transpose()
    }
}

/// `attempt_id`, as the recorded `outcome` of `operation_id` names it, while
/// the attempt's own row still agrees; `None` otherwise. A store from a
/// release that has the attempt migrations but doesn't record attempts, such
/// as an image-only rollback target, rewrites the outcome without touching
/// its `attempt_id`, so the row can name an attempt that didn't produce it.
/// This release writes a rollback or refusal and its attempt's row in one
/// transaction, with one failure and one time. So a rolled-back or refused
/// outcome names its attempt only while that attempt's row has the same
/// outcome, failure and time. Times are whole seconds, so a rewrite with the
/// same failure in the same second still names the old attempt. An
/// already-on-target outcome has no attempt row of its own, and names its
/// attempt unless that attempt's row says it rolled back or was refused.
fn recorded_outcome_attempt(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    attempt_id: String,
    outcome: RecordedRuntimeUpgradeOutcome,
    failure: Option<&str>,
    recorded_at: &str,
) -> Result<Option<String>> {
    let recorded: Option<(String, Option<String>, String)> = conn
        .query_row(
            "SELECT outcome, failure, recorded_at
               FROM sandbox_runtime_upgrade_attempt_outcomes
              WHERE operation_id = ?1
                AND attempt_id = ?2
                AND target_environment IS NOT NULL",
            params![operation_id.to_string(), attempt_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let recorded = recorded
        .map(|(attempt_outcome, attempt_failure, at)| {
            attempt_outcome
                .parse::<RecordedAttemptOutcome>()
                .map(|attempt_outcome| (attempt_outcome, attempt_failure, at))
                .map_err(|error| conversion_error(0, Type::Text, error))
        })
        .transpose()?;
    let agrees = match (outcome, recorded) {
        (
            RecordedRuntimeUpgradeOutcome::RolledBack,
            Some((RecordedAttemptOutcome::RolledBack, attempt_failure, at)),
        )
        | (
            RecordedRuntimeUpgradeOutcome::Refused,
            Some((RecordedAttemptOutcome::Refused, attempt_failure, at)),
        ) => attempt_failure.as_deref() == failure && at == recorded_at,
        (RecordedRuntimeUpgradeOutcome::RolledBack | RecordedRuntimeUpgradeOutcome::Refused, _) => {
            false
        }
        (
            RecordedRuntimeUpgradeOutcome::AlreadyOnTarget,
            Some((RecordedAttemptOutcome::RolledBack | RecordedAttemptOutcome::Refused, _, _)),
        ) => false,
        (RecordedRuntimeUpgradeOutcome::AlreadyOnTarget, _) => true,
    };
    Ok(agrees.then_some(attempt_id))
}

#[cfg(test)]
mod tests;
