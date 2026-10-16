//! Quarantine recovery: candidate scans, claim leases, and recovery outcomes.

use super::*;

/// Which recovery an eligible episode's evidence admits.
///
/// Node-local, so it lives here rather than in common-core beside the other
/// quarantine vocabularies: it is never persisted, never on the wire, has one
/// reader, and carries a `LocalArtifactId` that only this node's SQLite knows.
/// `QuarantineRecoveryNoProgressReason` is re-exported from common-core for the
/// opposite reason — its rendered form is written into
/// `sandbox_quarantine_state.last_error`.
///
/// Carried on the candidate because the gate below is the only place allowed to
/// decide it. Re-deriving it downstream from `sandboxes.current_local_artifact_id`
/// would make the branch a property of a mutable column rather than of the episode
/// the gate admitted, and two derivations only agree until one of them changes.
/// The reason code is fixed for an episode's lifetime; the column is not.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuarantineRecoveryBranch {
    /// A full suspend artifact is published on this node. Recovery validates that
    /// artifact and adopts the episode into a durable cleanup episode.
    ///
    /// Carries the adoption witness rather than a bare id, so the gate constructs it
    /// once and every consumer moves it. There is no fallible conversion left for a
    /// call site to get wrong or to write a dead arm around.
    AdoptLocalArtifact(AdoptableArtifact),
    /// The node holds no artifact for the sandbox at all, which the gate admits
    /// only for `OrphanedSandboxRowMissing`. Recovery re-stages the snapshot from
    /// remote storage — the multi-hour operation the node-wide permit bounds.
    RestoreOrphanedStaging,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuarantineRecoveryCandidate {
    pub sandbox_id: SandboxId,
    pub local_quarantine_id: uuid::Uuid,
    pub entered_at: u64,
    pub reason_code: SandboxQuarantineReasonCode,
    pub recovery: QuarantineRecoveryBranch,
    pub attempt_count: u32,
}

/// The one gate on legacy automatic recovery: a reason code that declares an
/// automatic recovery policy, plus artifact evidence matching that policy.
/// Returns which recovery that evidence admits, so no later reader can pick a
/// branch the evidence was never validated against.
///
/// The artifact-free branch is keyed on the reason code, not merely on the
/// absent artifact: `OrphanedSandboxRowMissing` is the only reason
/// `run_cross_node_restore_inner` will stage a snapshot for, so any other reason
/// admitted here would occupy the node-wide restore permit for a plan lookup and
/// a claim every backoff interval and never progress.
///
/// `sandbox_quarantine_state.next_action` is `requires_operator_attention`
/// for every sbx-manager quarantine, including all four recoverable reasons.
/// It records that a human may need to look, not that a human has taken
/// ownership, so it excludes nothing this gate admits.
fn quarantine_recovery_branch(
    state: SandboxState,
    reason_code: SandboxQuarantineReasonCode,
    local_artifact_id: Option<LocalArtifactId>,
    artifact_kind: Option<ArtifactKind>,
    artifact_origin: Option<ArtifactOrigin>,
) -> Option<QuarantineRecoveryBranch> {
    reason_code.automatic_recovery_policy()?;
    match local_artifact_id {
        Some(local_artifact_id) => (state == SandboxState::Quarantined
            && artifact_kind == Some(ArtifactKind::Full)
            && artifact_origin == Some(ArtifactOrigin::Suspend))
        .then_some(QuarantineRecoveryBranch::AdoptLocalArtifact(
            AdoptableArtifact(local_artifact_id),
        )),
        None => (reason_code == SandboxQuarantineReasonCode::OrphanedSandboxRowMissing
            && matches!(state, SandboxState::Quarantined | SandboxState::Restoring)
            && artifact_kind.is_none()
            && artifact_origin.is_none())
        .then_some(QuarantineRecoveryBranch::RestoreOrphanedStaging),
    }
}

/// Observations over the node-local quarantine cohort legacy recovery owns.
/// Completed periodic sweeps contain aggregate counts with an empty candidate
/// list: each bounded batch owns its candidates. Counts reflect each row when
/// visited, not a transactionally consistent inventory snapshot.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QuarantineRecoveryScan {
    pub candidates: Vec<QuarantineRecoveryCandidate>,
    pub pending_count: u64,
    pub pending_oldest_age: Duration,
    /// Rows in the legacy-owned cohort whose stored columns failed validation.
    /// They are skipped so valid siblings still recover. A genuine SQLite
    /// failure is not counted here — it aborts the scan, because a partial
    /// candidate list returned as success makes a failed pass look healthy.
    pub malformed_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// A candidate whose claim this process holds.
///
/// `candidate.attempt_count` is the count the claim wrote — post-claim, one above
/// what the scan read. There is deliberately no second copy: this type used to
/// carry its own `attempt_count` beside an embedded candidate still holding the
/// stale pre-claim value, and either compiled wherever a fence or a backoff was
/// built. The fence pins `attempt_count`, so picking the stale one would make
/// every fenced write of the operation miss, report a stale claim, and strand the
/// episode for its whole lease — silently, and only under a retry.
pub struct ClaimedQuarantineRecoveryCandidate {
    pub candidate: QuarantineRecoveryCandidate,
    pub claim_token: uuid::Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QuarantineRecoveryOwner {
    LegacyRecovery,
    OperatorAttention,
}

impl QuarantineRecoveryOwner {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LegacyRecovery => "legacy_recovery",
            Self::OperatorAttention => "operator_attention",
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unknown quarantine recovery owner {value:?}")]
pub(crate) struct QuarantineRecoveryOwnerParseError {
    value: String,
}

impl FromStr for QuarantineRecoveryOwner {
    type Err = QuarantineRecoveryOwnerParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "legacy_recovery" => Ok(Self::LegacyRecovery),
            "operator_attention" => Ok(Self::OperatorAttention),
            _ => Err(QuarantineRecoveryOwnerParseError {
                value: value.to_string(),
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuarantineRecoveryNoProgressOutcome {
    StaleClaim,
    RetryScheduled {
        attempt_count: u32,
        /// Seconds since the epoch, matching how the column is stored.
        next_attempt_at: i64,
    },
    /// This result completed `QUARANTINE_RECOVERY_PLAN_REJECTION_BOUND`
    /// consecutive plan rejections, so no retry was scheduled and the episode
    /// left the candidate set until it is re-armed.
    AutomaticRecoveryStopped {
        attempt_count: u32,
        consecutive_plan_rejections: u32,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawQuarantineRecoveryObservation {
    pub local_quarantine_id: String,
    pub owner: Option<String>,
    pub attempt_count: i64,
    pub next_attempt_at: Option<String>,
}

pub type RawQuarantineRecoveryObservationRead =
    std::result::Result<RawQuarantineRecoveryObservation, Error>;

pub fn is_orphan_quarantine_recovery_state(
    conn: &Connection,
    sandbox_id: &str,
    expected_state: SandboxState,
) -> Result<bool> {
    if !matches!(
        expected_state,
        SandboxState::Quarantined | SandboxState::Restoring
    ) {
        return Ok(false);
    }
    conn.query_row(
        "SELECT EXISTS (
             SELECT 1
               FROM sandboxes
               JOIN sandbox_quarantine_state qs ON qs.sandbox_id = sandboxes.id
              WHERE sandboxes.id = ?1
                AND sandboxes.state = ?2
                AND sandboxes.current_local_artifact_id IS NULL
                AND qs.source = ?3
                AND qs.reason_code = ?4
         )",
        params![
            sandbox_id,
            expected_state.as_str(),
            SandboxQuarantineSource::SbxManager.as_str(),
            SandboxQuarantineReasonCode::OrphanedSandboxRowMissing.as_str(),
        ],
        |row| row.get(0),
    )
}

/// Persists a cross-node staging failure on behalf of a recovery claim.
///
/// The target row is the claim's own sandbox, bound from the fence rather than
/// supplied alongside it. Taking the id separately let a caller fence one episode
/// and write another sandbox's row — every sibling fenced statement targets
/// `:fence_sandbox_id`, and this was the one exception.
///
/// The rootfs pull is a transfer, so a claim checked before it can be superseded
/// while it runs. Guarding the write on the claim inside the same statement is
/// what keeps a spent claimant from recording its failure as an ordinary
/// deferral on an episode that now belongs to someone else — a check before the
/// write would leave exactly that window open.
pub fn mark_cross_node_restore_staging_failed_under_recovery_claim(
    conn: &Connection,
    failure_reason: &str,
    claimed: &ClaimedQuarantineRecoveryCandidate,
) -> Result<bool> {
    let fence = ExactClaimFence::new(
        conn,
        claimed,
        now_epoch_i64()?,
        LeaseRequirement::MustBeLive,
        SandboxStateRequirement::MustBeActivatable,
        CleanupRequirement::MustBeAbsent,
    )?;
    let sql = format!(
        "UPDATE sandboxes
            SET failure_reason = :failure_reason,
                updated_at = :updated_at
          WHERE id = :fence_sandbox_id
            AND state = 'restoring'
            AND EXISTS (
                SELECT 1
                  FROM sandbox_quarantine_state AS qs
                 WHERE {}{}
            )",
        fence.quarantine_row_predicate("qs."),
        fence.sandbox_guards("qs.sandbox_id"),
    );
    let updated_at = now()?;
    let mut binds = fence.binds();
    binds.push((":failure_reason", &failure_reason));
    binds.push((":updated_at", &updated_at));
    let changed = conn.prepare_cached(&sql)?.execute(&binds[..])?;
    Ok(changed == 1)
}

/// Load node-local recovery scheduling facts separately from quarantine
/// classification metadata. The observation covers the automatic-recovery
/// reason class, including owners introduced by a newer release; artifact
/// eligibility is published independently and remains owned by
/// `quarantine_recovery_branch`.
pub fn quarantine_recovery_observations(
    conn: &Connection,
    sandbox_ids: &[String],
) -> Result<BTreeMap<String, RawQuarantineRecoveryObservationRead>> {
    let mut statement = conn.prepare_cached(
        "SELECT qs.sandbox_id, qs.quarantine_id, qs.next_action_owner,
                qs.attempt_count, qs.next_action_after, qs.reason_code
           FROM sandbox_quarantine_state qs
           JOIN sandboxes s ON s.id = qs.sandbox_id
           LEFT JOIN sandbox_cleanup_episodes cleanup ON cleanup.sandbox_id = s.id
          WHERE qs.sandbox_id = ?1
            AND qs.source = ?2
            AND s.state IN ('quarantined', 'restoring')
            AND cleanup.sandbox_id IS NULL",
    )?;
    let mut observations = BTreeMap::new();
    for sandbox_id in sandbox_ids {
        let observation = statement
            .query_row(
                params![sandbox_id, SandboxQuarantineSource::SbxManager.as_str()],
                |row| {
                    let reason_raw = row.get::<_, String>(5)?;
                    let reason =
                        reason_raw
                            .parse::<SandboxQuarantineReasonCode>()
                            .map_err(|error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    5,
                                    Type::Text,
                                    Box::new(error),
                                )
                            })?;
                    let observation = if reason.automatic_recovery_policy().is_some() {
                        Some(RawQuarantineRecoveryObservation {
                            local_quarantine_id: row.get(1)?,
                            owner: row.get(2)?,
                            attempt_count: row.get(3)?,
                            next_attempt_at: {
                                let value = row.get::<_, rusqlite::types::Value>(4)?;
                                let stored_type = value.data_type();
                                nullable_stored_timestamp_text(value).ok_or_else(|| {
                                    rusqlite::Error::InvalidColumnType(
                                        4,
                                        "next_action_after".into(),
                                        stored_type,
                                    )
                                })?
                            },
                        })
                    } else {
                        None
                    };
                    Ok((row.get::<_, String>(0)?, observation))
                },
            )
            .optional();
        match observation {
            Ok(Some((id, Some(observation)))) => {
                observations.insert(id, Ok(observation));
            }
            Ok(Some((_, None)) | None) => {}
            Err(error) => {
                observations.insert(sandbox_id.clone(), Err(error.into()));
            }
        }
    }
    Ok(observations)
}

/// One scanned row of the legacy-recovery quarantine cohort.
///
/// `Malformed` and `Ineligible` are both skipped and are deliberately different
/// nouns: a malformed row is stored data an operator has to look at, an
/// ineligible one is the ordinary case for most of the cohort.
pub(super) enum QuarantineRecoveryRow {
    Malformed,
    Ineligible,
    /// Eligible by its evidence, but recovery stopped after consecutive plan
    /// rejections. Still pending, so the backlog census keeps reporting the
    /// hold, but never due: it is an operator's to dispose of.
    RecoveryStopped {
        entered_at: u64,
    },
    Eligible {
        candidate: QuarantineRecoveryCandidate,
        /// The provisional retry time has arrived and no unexpired claim is held,
        /// so this pass may attempt the claim.
        due: bool,
    },
}

/// A stored column that must hold text, or the row is malformed.
///
/// The recovery tables are not `STRICT`. Converting inside `row.get` makes a
/// mistyped value a query error, and one query error retires every candidate in
/// the pass rather than the single bad row, so the wrong storage class has to be
/// classified exactly like an unparseable value.
fn required_stored_text(value: rusqlite::types::Value) -> Option<String> {
    match value {
        rusqlite::types::Value::Text(text) => Some(text),
        _ => None,
    }
}

/// A stored column that may hold text or SQL NULL. `None` means malformed, and
/// `Some(None)` means a legitimately absent value.
fn nullable_stored_text(value: rusqlite::types::Value) -> Option<Option<String>> {
    match value {
        rusqlite::types::Value::Text(text) => Some(Some(text)),
        rusqlite::types::Value::Null => Some(None),
        _ => None,
    }
}

/// Canonicalize valid historical encodings without hiding malformed diagnostic
/// text. Authoritative callers still validate the returned epoch seconds.
fn stored_timestamp_text(value: rusqlite::types::Value) -> Option<String> {
    timestamps::diagnostic_text(value)
}

pub(super) fn nullable_stored_timestamp_text(
    value: rusqlite::types::Value,
) -> Option<Option<String>> {
    match value {
        rusqlite::types::Value::Null => Some(None),
        value => stored_timestamp_text(value).map(Some),
    }
}

/// A stored column that must hold an integer, or the row is malformed.
fn required_stored_integer(value: rusqlite::types::Value) -> Option<i64> {
    match value {
        rusqlite::types::Value::Integer(integer) => Some(integer),
        _ => None,
    }
}

/// Scan the node-local quarantine cohort that legacy automatic recovery owns.
///
/// Returns the due candidates together with the backlog and malformed counts
/// derived from the same rows. Rows whose columns fail validation are counted
/// and skipped; a statement-level SQLite failure aborts the scan so a caller
/// can never mistake a partial read for an empty backlog.
#[cfg(test)]
pub fn scan_quarantine_recovery_candidates(conn: &Connection) -> Result<QuarantineRecoveryScan> {
    // Tests that need a whole cohort still exercise production eligibility,
    // paging and decoding. Stop at exactly one finite sweep.
    let mut cursor = PeriodicQuarantineRecoveryScan::default();
    let mut candidates = Vec::new();
    loop {
        let batch = cursor.next_batch(conn)?;
        candidates.extend(batch.candidates);
        if let Some(mut summary) = batch.completed_sweep {
            summary.candidates = candidates;
            return Ok(summary);
        }
    }
}

pub(super) fn decode_quarantine_recovery_row(
    row: &rusqlite::Row<'_>,
    current_epoch: i64,
) -> rusqlite::Result<QuarantineRecoveryRow> {
    // Read beside the decoded columns rather than among them: `last_error` is
    // diagnostic text with no validation, so a value it cannot parse only
    // means no stop is recorded, never a malformed row.
    let stopped = automatic_recovery_stopped(&row.get::<_, rusqlite::types::Value>(14)?);
    // Read the stored values without converting. These tables are not
    // `STRICT`, so any column can hold any storage class, and asking
    // rusqlite to convert would turn one mistyped value into a query
    // error that retires every candidate in the pass. `?` here fires only
    // on a genuine SQLite failure, which must still abort.
    // Owned and consumed, not borrowed and cloned. Reading each column
    // into a `Value` and then cloning the `String` out of it allocated
    // twice per text column per row; moving it out allocates once.
    let stored: [rusqlite::types::Value; 12] = [
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
        row.get(10)?,
        row.get(11)?,
    ];
    let [
        stored_id,
        stored_quarantine_id,
        stored_entered_at,
        stored_state,
        stored_reason,
        stored_artifact_id,
        stored_artifact_kind,
        stored_artifact_origin,
        stored_attempts,
        stored_next_action,
        stored_claim_token,
        stored_claim_expiry,
    ] = stored;
    let (
        Some(sandbox_id_raw),
        Some(local_quarantine_id_raw),
        Some(entered_at_raw),
        Some(state_raw),
        Some(reason_code_raw),
        Some(local_artifact_id_raw),
        Some(artifact_kind_raw),
        Some(artifact_origin_raw),
        Some(attempt_count_raw),
        Some(next_action_after_raw),
        Some(claim_token_raw),
        Some(claim_expires_at_raw),
    ) = (
        required_stored_text(stored_id),
        required_stored_text(stored_quarantine_id),
        stored_timestamp_text(stored_entered_at),
        required_stored_text(stored_state),
        required_stored_text(stored_reason),
        nullable_stored_text(stored_artifact_id),
        nullable_stored_text(stored_artifact_kind),
        nullable_stored_text(stored_artifact_origin),
        required_stored_integer(stored_attempts),
        nullable_stored_timestamp_text(stored_next_action),
        nullable_stored_text(stored_claim_token),
        nullable_stored_timestamp_text(stored_claim_expiry),
    )
    else {
        tracing::warn!(
            "skipping quarantine recovery row whose stored columns have \
             unexpected storage classes"
        );
        return Ok(QuarantineRecoveryRow::Malformed);
    };
    // Everything above is a read of the stored column and propagates a
    // genuine rusqlite failure. Everything below is validation of what
    // was read, and an unparseable value yields `None` so the row is
    // counted as malformed rather than aborting the pass.
    let (
        Ok(sandbox_id),
        Ok(local_quarantine_id),
        Ok(entered_at),
        Ok(state),
        Ok(reason_code),
        Ok(local_artifact_id),
        Ok(artifact_kind),
        Ok(artifact_origin),
        Ok(attempt_count),
        Ok(next_action_after),
        Ok(claim_expires_at),
    ) = (
        sandbox_id_raw.parse::<SandboxId>(),
        local_quarantine_id_raw.parse::<uuid::Uuid>(),
        timestamps::EpochSeconds::parse(&entered_at_raw).map(timestamps::EpochSeconds::unsigned),
        state_raw.parse::<SandboxState>(),
        reason_code_raw.parse::<SandboxQuarantineReasonCode>(),
        local_artifact_id_raw
            .as_deref()
            .map(LocalArtifactId::from_str)
            .transpose(),
        artifact_kind_raw
            .as_deref()
            .map(ArtifactKind::from_str)
            .transpose(),
        artifact_origin_raw
            .as_deref()
            .map(ArtifactOrigin::from_str)
            .transpose(),
        u32::try_from(attempt_count_raw),
        next_action_after_raw
            .as_deref()
            .map(|value| timestamps::EpochSeconds::parse(value).map(timestamps::EpochSeconds::get))
            .transpose(),
        claim_expires_at_raw
            .as_deref()
            .map(|value| timestamps::EpochSeconds::parse(value).map(timestamps::EpochSeconds::get))
            .transpose(),
    )
    else {
        tracing::warn!(
            sandbox_id = %sandbox_id_raw,
            quarantine_id = %local_quarantine_id_raw,
            "skipping malformed quarantine recovery row"
        );
        return Ok(QuarantineRecoveryRow::Malformed);
    };
    let Some(recovery) = quarantine_recovery_branch(
        state,
        reason_code,
        local_artifact_id,
        artifact_kind,
        artifact_origin,
    ) else {
        return Ok(QuarantineRecoveryRow::Ineligible);
    };
    if stopped {
        return Ok(QuarantineRecoveryRow::RecoveryStopped { entered_at });
    }
    // A detached orphan restore runs outside the serial inventory pass, so
    // this lease is also the single-dispatch guard on the next one. Its
    // heartbeat extends the guard; a lost lease admits a new claimant, and
    // the superseded task's exact-token fence is what stops it mutating.
    let claim_is_live = claim_token_raw.is_some()
        && claim_expires_at.is_some_and(|expires_at| expires_at > current_epoch);
    let retry_time_reached =
        next_action_after.is_none_or(|next_attempt_at| next_attempt_at <= current_epoch);
    Ok(QuarantineRecoveryRow::Eligible {
        candidate: QuarantineRecoveryCandidate {
            sandbox_id,
            local_quarantine_id,
            entered_at,
            reason_code,
            recovery,
            attempt_count,
        },
        due: retry_time_reached && !claim_is_live,
    })
}

pub(crate) const QUARANTINE_RECOVERY_CLAIM_LEASE: Duration = Duration::from_secs(15 * 60);

pub(crate) const QUARANTINE_RECOVERY_BACKOFF_BASE: Duration = Duration::from_secs(60);

pub(super) const QUARANTINE_RECOVERY_BACKOFF_MAX: Duration = Duration::from_secs(15 * 60);

/// Consecutive `plan_ineligible` or `plan_not_found` results after which legacy
/// recovery stops scheduling an episode and leaves it to an operator.
///
/// The control plane answers a plan only for its own matching quarantine, so an
/// episode it keeps rejecting, such as an orphan hold with no cluster
/// quarantine, can never recover. Retrying it forever costs a plan lookup per
/// backoff and takes a per-pass admission from episodes that can.
///
/// Sixteen results span the 15 intervals between them. From a fresh episode
/// those follow the backoff: 48–60 s, 96–120 s, 192–240 s, 384–480 s, then
/// eleven capped 720–900 s intervals, which is 2.4–3 h plus up to one 60 s tick
/// each. An episode already at the cap needs 3–3.75 h. A rolling deploy of the
/// API, or a node republishing its quarantine after an outage, resolves in
/// minutes. The outage itself reports lookup failures or timeouts, which leave
/// a run where it was instead of extending it.
pub(crate) const QUARANTINE_RECOVERY_PLAN_REJECTION_BOUND: u32 = 16;

/// How the node-local SQLite `sandbox_quarantine_state.last_error` carries a
/// run of plan rejections. A first rejection records its bare reason, as every
/// other result does; a repeat appends the run length. Older binaries write
/// only the bare reason, which reads as a run of one.
const PLAN_REJECTION_RUN_SEPARATOR: &str = ";consecutive_plan_rejections=";

/// What one resolved plan lookup, or one recorded result, does to the run of
/// consecutive plan rejections.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlanRejectionRunEffect {
    /// The control plane answered that it has no plan for this episode.
    Extends,
    /// The control plane did not answer, so the run stays where it was. An
    /// outage therefore can neither extend a run nor restart one.
    Leaves,
    /// Anything else resolved the attempt: a plan, a refused or malformed one,
    /// or a local outcome after a plan was accepted.
    Ends,
}

impl PlanRejectionRunEffect {
    /// Every reason is named, so a new one cannot compile until it is placed.
    pub const fn of(reason: QuarantineRecoveryNoProgressReason) -> Self {
        use QuarantineRecoveryNoProgressReason as Reason;
        match reason {
            Reason::PlanIneligible | Reason::PlanNotFound => Self::Extends,
            Reason::PlanLookupFailed | Reason::PlanLookupTimedOut => Self::Leaves,
            Reason::PlanRpcUnsupported
            | Reason::PlanMalformed
            | Reason::UnsupportedTargetState
            | Reason::PlanReasonIneligible
            | Reason::LocalReasonMismatch
            | Reason::LocalArtifactMismatch
            | Reason::PlanArtifactMismatch
            | Reason::ClaimRenewalFailed
            | Reason::NodeDatabaseFailure
            | Reason::NodeFilesystemFailure
            | Reason::RestoreEvidenceMismatch
            | Reason::ControlPlaneRefused
            | Reason::CleanupFenceBusy
            | Reason::LocalArtifactInspectionFailed
            | Reason::LocalAdoptionFenceChanged
            | Reason::LocalAdoptionFailed
            | Reason::OrphanRestoreLocalContention
            | Reason::OrphanRestoreTaskFailed
            | Reason::OrphanRestoreDeferred
            | Reason::OrphanRestoreTimedOut
            | Reason::ClaimSuperseded
            | Reason::ClaimPreconditionsChanged
            | Reason::ClaimAcquisitionFailed
            | Reason::AbandonedAtShutdown => Self::Ends,
        }
    }
}

/// The run of consecutive plan rejections that ends in this `last_error`, and
/// the reason that ended it. Any other result, an unparseable count, or a
/// non-text value ends the run, so corrupt bookkeeping resumes retries rather
/// than stopping them.
///
/// The run lives in `last_error` because that column already holds the latest
/// resolved plan answer. A lookup failure or timeout inside a run leaves the
/// rejection text in place, so it shows only in the metric and the log.
/// `attempt_count` is the backoff curve's position and is deliberately
/// never reset on a changed outcome: counting the run there would either reset
/// the curve whenever outcomes alternate, or let one rejection after a long
/// history of other failures stop a recoverable episode.
fn plan_rejection_run(
    last_error: &rusqlite::types::Value,
) -> Option<(QuarantineRecoveryNoProgressReason, u32)> {
    let rusqlite::types::Value::Text(last_error) = last_error else {
        return None;
    };
    let (reason, run) = match last_error.split_once(PLAN_REJECTION_RUN_SEPARATOR) {
        Some((reason, run)) => (reason, run.parse::<u32>().ok()?),
        None => (last_error.as_str(), 1),
    };
    let reason = [
        QuarantineRecoveryNoProgressReason::PlanIneligible,
        QuarantineRecoveryNoProgressReason::PlanNotFound,
    ]
    .into_iter()
    .find(|rejection| rejection.as_str() == reason)?;
    (run > 0).then_some((reason, run))
}

fn plan_rejection_last_error(reason: QuarantineRecoveryNoProgressReason, run: u32) -> String {
    if run <= 1 {
        reason.as_str().to_owned()
    } else {
        format!("{reason}{PLAN_REJECTION_RUN_SEPARATOR}{run}")
    }
}

/// The one place the bound is compared.
const fn run_stops_recovery(run: u32) -> bool {
    run >= QUARANTINE_RECOVERY_PLAN_REJECTION_BOUND
}

/// Whether legacy recovery stopped scheduling the episode with this `last_error`.
pub(super) fn automatic_recovery_stopped(last_error: &rusqlite::types::Value) -> bool {
    plan_rejection_run(last_error).is_some_and(|(_, run)| run_stops_recovery(run))
}

/// Re-arm every stopped episode in the node-local SQLite, allowing one more
/// plan rejection before it stops again.
///
/// Called once at startup. A stopped episode never asks the control plane
/// again, so a restart is when it learns whether a plan has appeared since: the
/// run is set one short of the bound, so another rejection stops it again at
/// once, any other resolved answer ends the run and resumes ordinary recovery,
/// and lookup failures leave it one short and keep retrying at the capped
/// backoff. An operator re-arms a node's holds by restarting its
/// sandbox manager.
pub fn rearm_stopped_quarantine_recoveries(conn: &Connection) -> Result<usize> {
    let stopped = {
        let mut statement = conn.prepare(
            "SELECT sandbox_id, quarantine_id, last_error
               FROM sandbox_quarantine_state
              WHERE source = ?1
                AND (next_action_owner IS NULL OR next_action_owner = ?2)
                AND claim_token IS NULL
                AND last_error LIKE ?3",
        )?;
        let rows = statement.query_map(
            params![
                SandboxQuarantineSource::SbxManager.as_str(),
                QuarantineRecoveryOwner::LegacyRecovery.as_str(),
                format!("%{PLAN_REJECTION_RUN_SEPARATOR}%"),
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, rusqlite::types::Value>(2)?,
                ))
            },
        )?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    let updated_at = now_epoch_i64()?;
    let mut rearmed = 0;
    for (sandbox_id, quarantine_id, last_error) in stopped {
        let Some((reason, run)) = plan_rejection_run(&last_error) else {
            continue;
        };
        if !run_stops_recovery(run) {
            continue;
        }
        let probe = plan_rejection_last_error(
            reason,
            QUARANTINE_RECOVERY_PLAN_REJECTION_BOUND.saturating_sub(1),
        );
        rearmed += conn.execute(
            "UPDATE sandbox_quarantine_state
                SET last_error = ?1,
                    updated_at = ?2
              WHERE sandbox_id = ?3
                AND quarantine_id = ?4
                AND claim_token IS NULL",
            params![probe, updated_at, sandbox_id, quarantine_id],
        )?;
    }
    Ok(rearmed)
}

/// Seconds since the epoch, for the SQL boundary where quarantine timestamps
/// are stored as decimal strings.
fn epoch_secs_after(current_epoch: i64, span: Duration) -> i64 {
    current_epoch.saturating_add(i64::try_from(span.as_secs()).unwrap_or(i64::MAX))
}

/// Whether a fenced statement still requires the claim's lease to be unexpired.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LeaseRequirement {
    MustBeLive,
    /// The claim token is minted per acquisition, so a lease that expired and
    /// was re-claimed is already caught by the token. Only statements that
    /// deliberately act after a lease may have lapsed use this.
    MayHaveExpired,
}

/// Whether a fenced statement requires the sandbox to still be activatable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SandboxStateRequirement {
    MustBeActivatable,
    AnyState,
}

/// Whether a fenced statement requires no durable cleanup episode to own the
/// sandbox.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CleanupRequirement {
    MustBeAbsent,
    MayExist,
}

/// One exact legacy-recovery claim, rendered as a SQL predicate.
///
/// Every statement that commits under a claim shares this predicate instead of
/// spelling it out. The seven identity conjuncts are not optional: a claimant
/// whose episode changed source, reason, owner, token, or attempt count must not
/// be able to write. The three environmental guards are per statement and each
/// relaxation has to name itself, so a missing guard is a visible argument at
/// the call site rather than an omission buried in a forty-line string.
pub(super) struct ExactClaimFence {
    sandbox_id: String,
    quarantine_id: String,
    source: &'static str,
    reason_code: &'static str,
    owner: &'static str,
    claim_token: String,
    attempt_count: u32,
    live_expiry: Option<rusqlite::types::Value>,
    lease: LeaseRequirement,
    sandbox_state: SandboxStateRequirement,
    cleanup: CleanupRequirement,
}

impl ExactClaimFence {
    pub(super) fn new(
        conn: &Connection,
        claimed: &ClaimedQuarantineRecoveryCandidate,
        now_epoch: i64,
        lease: LeaseRequirement,
        sandbox_state: SandboxStateRequirement,
        cleanup: CleanupRequirement,
    ) -> Result<Self> {
        // SQLite numeric CAST accepts prefixes and saturates overflow. Validate
        // the deadline in Rust, then bind its original storage value so a change
        // between this read and the guarded statement cannot reuse that proof.
        let live_expiry = if lease == LeaseRequirement::MustBeLive {
            conn.query_row(
                "SELECT claim_expires_at FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
                [claimed.candidate.sandbox_id.to_string()],
                |row| row.get::<_, rusqlite::types::Value>(0),
            )
            .optional()?
            .filter(|value| {
                timestamps::EpochSeconds::decode(value).is_ok_and(|value| value.get() > now_epoch)
            })
        } else {
            None
        };
        Ok(Self {
            sandbox_id: claimed.candidate.sandbox_id.to_string(),
            quarantine_id: claimed.candidate.local_quarantine_id.to_string(),
            source: SandboxQuarantineSource::SbxManager.as_str(),
            reason_code: claimed.candidate.reason_code.as_str(),
            owner: QuarantineRecoveryOwner::LegacyRecovery.as_str(),
            claim_token: claimed.claim_token.to_string(),
            attempt_count: claimed.candidate.attempt_count,
            live_expiry,
            lease,
            sandbox_state,
            cleanup,
        })
    }

    /// The predicate identifying the quarantine row, qualified by `alias`
    /// (`"qs."` when the statement joins it, `""` when it is the update target).
    pub(super) fn quarantine_row_predicate(&self, alias: &str) -> String {
        let mut clauses = vec![
            format!("{alias}sandbox_id = :fence_sandbox_id"),
            format!("{alias}quarantine_id = :fence_quarantine_id"),
            format!("{alias}source = :fence_source"),
            format!("{alias}reason_code = :fence_reason_code"),
            format!("{alias}next_action_owner = :fence_owner"),
            format!("{alias}claim_token = :fence_claim_token"),
            format!("{alias}attempt_count = :fence_attempt_count"),
        ];
        if self.lease == LeaseRequirement::MustBeLive {
            clauses.push(format!("{alias}claim_expires_at = :fence_live_expiry"));
        }
        clauses.join("\n            AND ")
    }

    /// The guards on the sandbox that the claim's episode belongs to, where
    /// `sandbox_ref` is the SQL expression naming its id.
    fn sandbox_guards(&self, sandbox_ref: &str) -> String {
        let mut clauses = Vec::new();
        if self.sandbox_state == SandboxStateRequirement::MustBeActivatable {
            clauses.push(format!(
                "EXISTS (
                SELECT 1 FROM sandboxes fence_s
                 WHERE fence_s.id = {sandbox_ref}
                   AND fence_s.state IN ('quarantined', 'restoring')
            )"
            ));
        }
        if self.cleanup == CleanupRequirement::MustBeAbsent {
            clauses.push(format!(
                "NOT EXISTS (
                SELECT 1 FROM sandbox_cleanup_episodes fence_cleanup
                 WHERE fence_cleanup.sandbox_id = {sandbox_ref}
            )"
            ));
        }
        if clauses.is_empty() {
            return String::new();
        }
        format!("\n            AND {}", clauses.join("\n            AND "))
    }

    /// The binds for exactly the names the emitted predicate references.
    ///
    /// SQLite rejects a bind for a name the statement does not mention, so this
    /// has to track `quarantine_row_predicate` and cannot supply a superset.
    pub(super) fn binds(&self) -> Vec<(&'static str, &dyn rusqlite::ToSql)> {
        let mut binds: Vec<(&'static str, &dyn rusqlite::ToSql)> = vec![
            (":fence_sandbox_id", &self.sandbox_id),
            (":fence_quarantine_id", &self.quarantine_id),
            (":fence_source", &self.source),
            (":fence_reason_code", &self.reason_code),
            (":fence_owner", &self.owner),
            (":fence_claim_token", &self.claim_token),
            (":fence_attempt_count", &self.attempt_count),
        ];
        if self.lease == LeaseRequirement::MustBeLive {
            binds.push((":fence_live_expiry", &self.live_expiry));
        }
        binds
    }
}

/// Claim one due episode in the node-local SQLite `sandbox_quarantine_state`.
///
/// `plan` is what the attempt's plan lookup, which runs before the claim, did
/// to the run of plan rejections. An answer that ends the run ends it here, in
/// the claim's own statement: an accepted plan dispatches work whose outcome a
/// panicking or cancelled task never records, and the rejections either side
/// of it are not consecutive.
pub fn claim_quarantine_recovery_candidate(
    conn: &Connection,
    candidate: &QuarantineRecoveryCandidate,
    plan: PlanRejectionRunEffect,
) -> Result<Option<ClaimedQuarantineRecoveryCandidate>> {
    let current_epoch = now_epoch_i64()?;
    let deadlines = conn
        .query_row(
            "SELECT next_action_after, claim_token, claim_expires_at, last_error
         FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
            [candidate.sandbox_id.to_string()],
            |row| {
                Ok((
                    row.get::<_, rusqlite::types::Value>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, rusqlite::types::Value>(2)?,
                    row.get::<_, rusqlite::types::Value>(3)?,
                ))
            },
        )
        .optional()?;
    let Some((next_action, previous_token, previous_expiry, last_error)) = deadlines else {
        return Ok(None);
    };
    // A stopped episode has no retry time, which reads as due. Refusing it here
    // keeps the claim authoritative rather than trusting the scan.
    if automatic_recovery_stopped(&last_error) {
        return Ok(None);
    }
    // Only the run is cleared; `attempt_count` and the backoff advance as for
    // any claim, and a `last_error` that holds no run is kept.
    let last_error =
        if plan == PlanRejectionRunEffect::Ends && plan_rejection_run(&last_error).is_some() {
            rusqlite::types::Value::Null
        } else {
            last_error
        };
    let due = matches!(next_action, rusqlite::types::Value::Null)
        || timestamps::EpochSeconds::decode(&next_action)
            .is_ok_and(|value| value.get() <= current_epoch);
    let expiry = match &previous_expiry {
        rusqlite::types::Value::Null => None,
        value => match timestamps::EpochSeconds::decode(value) {
            Ok(value) => Some(value.get()),
            Err(_) => return Ok(None),
        },
    };
    if !due || (previous_token.is_some() && expiry.is_some_and(|value| value > current_epoch)) {
        return Ok(None);
    }
    let claim_expires_at = epoch_secs_after(current_epoch, QUARANTINE_RECOVERY_CLAIM_LEASE);
    let claim_token = uuid::Uuid::now_v7();
    let attempt_count = candidate.attempt_count.saturating_add(1);
    let next_attempt_at = epoch_secs_after(
        claim_expires_at,
        quarantine_recovery_backoff(candidate.local_quarantine_id, attempt_count),
    );
    let changed = conn.execute(
        "UPDATE sandbox_quarantine_state AS qs
            SET next_action_owner = ?1,
                claim_token = ?2,
                claim_expires_at = ?3,
                attempt_count = ?4,
                next_action_after = ?5,
                updated_at = ?6,
                last_error = ?15
          WHERE qs.sandbox_id = ?7
            AND qs.quarantine_id = ?8
            AND qs.source = ?9
            AND qs.reason_code = ?10
            AND (qs.next_action_owner IS NULL OR qs.next_action_owner = ?1)
            AND qs.next_action_after IS ?12
            AND qs.claim_expires_at IS ?13
            AND qs.claim_token IS ?14
            AND qs.attempt_count = ?11
            AND EXISTS (
                SELECT 1 FROM sandboxes s
                 WHERE s.id = qs.sandbox_id
                   AND s.state IN ('quarantined', 'restoring')
            )
            AND NOT EXISTS (
                SELECT 1 FROM sandbox_cleanup_episodes cleanup
                 WHERE cleanup.sandbox_id = qs.sandbox_id
            )",
        params![
            QuarantineRecoveryOwner::LegacyRecovery.as_str(),
            claim_token.to_string(),
            claim_expires_at.to_string(),
            attempt_count,
            next_attempt_at.to_string(),
            current_epoch,
            candidate.sandbox_id.to_string(),
            candidate.local_quarantine_id.to_string(),
            SandboxQuarantineSource::SbxManager.as_str(),
            candidate.reason_code.as_str(),
            candidate.attempt_count,
            next_action,
            previous_expiry,
            previous_token,
            last_error,
        ],
    )?;
    Ok((changed == 1).then(|| ClaimedQuarantineRecoveryCandidate {
        candidate: QuarantineRecoveryCandidate {
            attempt_count,
            ..candidate.clone()
        },
        claim_token,
    }))
}

pub fn renew_quarantine_recovery_claim(
    conn: &Connection,
    claimed: &ClaimedQuarantineRecoveryCandidate,
) -> Result<bool> {
    let current_epoch = now_epoch_i64()?;
    let claim_expires_at = epoch_secs_after(current_epoch, QUARANTINE_RECOVERY_CLAIM_LEASE);
    let next_attempt_at = epoch_secs_after(
        claim_expires_at,
        quarantine_recovery_backoff(
            claimed.candidate.local_quarantine_id,
            claimed.candidate.attempt_count,
        ),
    );
    let fence = ExactClaimFence::new(
        conn,
        claimed,
        current_epoch,
        LeaseRequirement::MustBeLive,
        SandboxStateRequirement::MustBeActivatable,
        CleanupRequirement::MustBeAbsent,
    )?;
    let claim_expires_at = claim_expires_at.to_string();
    let next_attempt_at = next_attempt_at.to_string();
    let sql = format!(
        "UPDATE sandbox_quarantine_state
            SET claim_expires_at = :claim_expires_at,
                next_action_after = :next_attempt_at,
                updated_at = :updated_at
          WHERE {}{}",
        fence.quarantine_row_predicate(""),
        fence.sandbox_guards("sandbox_quarantine_state.sandbox_id"),
    );
    let mut binds: Vec<(&str, &dyn rusqlite::ToSql)> = vec![
        (":claim_expires_at", &claim_expires_at),
        (":next_attempt_at", &next_attempt_at),
        (":updated_at", &current_epoch),
    ];
    binds.extend(fence.binds());
    let changed = conn.prepare_cached(&sql)?.execute(&binds[..])?;
    Ok(changed == 1)
}

pub fn quarantine_recovery_claim_is_current(
    conn: &Connection,
    claimed: &ClaimedQuarantineRecoveryCandidate,
) -> Result<bool> {
    let fence = ExactClaimFence::new(
        conn,
        claimed,
        now_epoch_i64()?,
        LeaseRequirement::MustBeLive,
        SandboxStateRequirement::MustBeActivatable,
        CleanupRequirement::MustBeAbsent,
    )?;
    let sql = format!(
        "SELECT EXISTS (
             SELECT 1
               FROM sandbox_quarantine_state AS qs
              WHERE {}{}
         )",
        fence.quarantine_row_predicate("qs."),
        fence.sandbox_guards("qs.sandbox_id"),
    );
    let mut statement = conn.prepare_cached(&sql)?;
    Ok(statement.query_row(&fence.binds()[..], |row| row.get(0))?)
}

fn quarantine_recovery_backoff(local_quarantine_id: uuid::Uuid, attempt_count: u32) -> Duration {
    let shift = attempt_count.saturating_sub(1).min(20);
    let capped = QUARANTINE_RECOVERY_BACKOFF_BASE
        .saturating_mul(1_u32 << shift)
        .min(QUARANTINE_RECOVERY_BACKOFF_MAX);
    let jitter_range = capped / 5;
    if jitter_range.is_zero() {
        return capped;
    }
    let seed = local_quarantine_id
        .as_u128()
        .wrapping_add(u128::from(attempt_count));
    let offset = u64::try_from(seed % u128::from(jitter_range.as_secs().saturating_add(1)))
        .unwrap_or_default();
    capped.saturating_sub(Duration::from_secs(offset))
}

pub fn record_quarantine_recovery_no_progress(
    conn: &Connection,
    claimed: &ClaimedQuarantineRecoveryCandidate,
    reason: QuarantineRecoveryNoProgressReason,
) -> Result<QuarantineRecoveryNoProgressOutcome> {
    let current_epoch = now_epoch_i64()?;
    let previous_last_error = conn
        .query_row(
            "SELECT last_error FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
            [claimed.candidate.sandbox_id.to_string()],
            |row| row.get::<_, rusqlite::types::Value>(0),
        )
        .optional()?
        .unwrap_or(rusqlite::types::Value::Null);
    let previous_run = plan_rejection_run(&previous_last_error);
    let (last_error, plan_rejections) = match PlanRejectionRunEffect::of(reason) {
        PlanRejectionRunEffect::Extends => {
            let run = previous_run.map_or(0, |(_, run)| run).saturating_add(1);
            (plan_rejection_last_error(reason, run), run)
        }
        // The run stays as it was; the metric and the log carry this reason.
        PlanRejectionRunEffect::Leaves => match previous_run {
            Some((rejection, run)) => (plan_rejection_last_error(rejection, run), run),
            None => (reason.as_str().to_owned(), 0),
        },
        PlanRejectionRunEffect::Ends => (reason.as_str().to_owned(), 0),
    };
    // A run that reaches the bound only does so by extending: the claim refuses
    // an episode that is already stopped.
    let stops = run_stops_recovery(plan_rejections);
    let next_attempt_at = epoch_secs_after(
        current_epoch,
        quarantine_recovery_backoff(
            claimed.candidate.local_quarantine_id,
            claimed.candidate.attempt_count,
        ),
    );
    // The seven identity conjuncts are what prove this process still owns the episode,
    // and they stay pinned. The three environmental guards are relaxed deliberately:
    // this statement's whole job is to record why an attempt made no progress and when
    // to try again, and requiring a live lease, an activatable sandbox and no cleanup
    // episode made it miss in exactly the situations it exists to report. A cleanup
    // owner arriving after the claim then left the lease standing for its full 15
    // minutes instead of one 48-60s backoff, and the episode out of the candidate set
    // with nothing recorded.
    //
    // There used to be a second statement with this identical SET clause and these
    // relaxations, reached only from the two call sites that had noticed the problem.
    // Two writers with the same effect and different preconditions make "which one did
    // this site pick" invisible to every statement-level test, which is why nine call
    // sites kept the strict one and reviewers found them one at a time.
    // `no_two_fenced_writers_share_a_column_set_under_different_guards` now refuses the
    // pair.
    let fence = ExactClaimFence::new(
        conn,
        claimed,
        current_epoch,
        LeaseRequirement::MayHaveExpired,
        SandboxStateRequirement::AnyState,
        CleanupRequirement::MayExist,
    )?;
    // A stopped episode keeps no retry time: nothing is scheduled for it. The
    // run read above needs no pin: the caller holds the node's one SQLite
    // connection across both statements.
    let next_attempt_at_text = (!stops).then(|| next_attempt_at.to_string());
    let sql = format!(
        "UPDATE sandbox_quarantine_state
            SET next_action_after = :next_attempt_at,
                claim_token = NULL,
                claim_expires_at = NULL,
                last_error = :last_error,
                updated_at = :updated_at
          WHERE {}",
        fence.quarantine_row_predicate(""),
    );
    let mut binds: Vec<(&str, &dyn rusqlite::ToSql)> = vec![
        (":next_attempt_at", &next_attempt_at_text),
        (":last_error", &last_error),
        (":updated_at", &current_epoch),
    ];
    binds.extend(fence.binds());
    let changed = conn.prepare_cached(&sql)?.execute(&binds[..])?;
    if changed != 1 {
        return Ok(QuarantineRecoveryNoProgressOutcome::StaleClaim);
    }
    if stops {
        return Ok(
            QuarantineRecoveryNoProgressOutcome::AutomaticRecoveryStopped {
                attempt_count: claimed.candidate.attempt_count,
                consecutive_plan_rejections: plan_rejections,
            },
        );
    }
    Ok(QuarantineRecoveryNoProgressOutcome::RetryScheduled {
        attempt_count: claimed.candidate.attempt_count,
        next_attempt_at,
    })
}

pub fn set_orphan_quarantine_recovery_generation(
    conn: &Connection,
    claimed: &ClaimedQuarantineRecoveryCandidate,
    execution_generation: ExecutionGeneration,
) -> Result<bool> {
    let current_epoch = now_epoch_i64()?;
    // `state = 'restoring'` below is stricter than the fence's activatable
    // check, so the fence does not repeat it.
    let fence = ExactClaimFence::new(
        conn,
        claimed,
        current_epoch,
        LeaseRequirement::MustBeLive,
        SandboxStateRequirement::AnyState,
        CleanupRequirement::MustBeAbsent,
    )?;
    let generation = execution_generation.get();
    let sql = format!(
        "UPDATE sandboxes
            SET space_api_route_execution_generation = :generation,
                updated_at = :updated_at
          WHERE id = :fence_sandbox_id
            AND state = 'restoring'
            AND current_local_artifact_id IS NULL
            AND EXISTS (
                SELECT 1 FROM sandbox_quarantine_state qs
                 WHERE {}
            ){}",
        fence.quarantine_row_predicate("qs."),
        fence.sandbox_guards("sandboxes.id"),
    );
    let mut binds: Vec<(&str, &dyn rusqlite::ToSql)> = vec![
        (":generation", &generation),
        (":updated_at", &current_epoch),
    ];
    binds.extend(fence.binds());
    let changed = conn.prepare_cached(&sql)?.execute(&binds[..])?;
    Ok(changed == 1)
}

pub fn publish_orphan_quarantine_recovered_artifact(
    conn: &Connection,
    claimed: &ClaimedQuarantineRecoveryCandidate,
    local_artifact_id: Option<LocalArtifactId>,
    local_path: &str,
    execution_generation: ExecutionGeneration,
    receipt_id: SandboxCleanupEpisodeId,
) -> Result<bool> {
    let sandbox_id = claimed.candidate.sandbox_id;
    let sandbox_id_string = sandbox_id.to_string();
    let reason_code = SandboxQuarantineReasonCode::OrphanedSandboxRowMissing;
    let timestamp = now_epoch_i64()?;
    // `state = 'restoring'` is stricter than the fence's activatable check, so
    // the fence does not repeat it.
    let fence = ExactClaimFence::new(
        conn,
        claimed,
        timestamp,
        LeaseRequirement::MustBeLive,
        SandboxStateRequirement::AnyState,
        CleanupRequirement::MustBeAbsent,
    )?;
    let local_artifact_id_text = local_artifact_id.map(|id| id.to_string());
    let receipt_id_text = receipt_id.to_string();
    let generation = execution_generation.get();
    let reason_code_text = reason_code.as_str();
    let publish_sql = format!(
        "UPDATE sandboxes
            SET state = 'suspended',
                restore_origin = NULL,
                restore_phase = NULL,
                restore_storage_uri = NULL,
                failure_reason = NULL,
                current_local_artifact_id = :local_artifact_id,
                current_local_artifact_kind = CASE WHEN :local_artifact_id IS NULL THEN NULL ELSE 'full' END,
                current_local_artifact_origin = CASE WHEN :local_artifact_id IS NULL THEN NULL ELSE 'suspend' END,
                transition_receipt_id = :receipt_id,
                transition_receipt_kind = 'quarantine_recovery',
                transition_receipt_target_state = 'suspended',
                transition_receipt_execution_generation = :generation,
                transition_receipt_activation_attempt_id = NULL,
                transition_receipt_reason_code = :reason_code,
                state_changed_at = CASE
                    WHEN state != 'suspended' THEN :timestamp
                    ELSE state_changed_at
                END,
                updated_at = :timestamp
          WHERE id = :fence_sandbox_id
            AND state = 'restoring'
            AND current_local_artifact_id IS NULL
            AND space_api_route_execution_generation = :generation
            AND EXISTS (
                SELECT 1 FROM sandbox_quarantine_state qs
                 WHERE {}
            ){}",
        fence.quarantine_row_predicate("qs."),
        fence.sandbox_guards("sandboxes.id"),
    );
    let tx = immediate_transaction(conn)?;
    let changed = {
        let mut binds: Vec<(&str, &dyn rusqlite::ToSql)> = vec![
            (":local_artifact_id", &local_artifact_id_text),
            (":receipt_id", &receipt_id_text),
            (":generation", &generation),
            (":reason_code", &reason_code_text),
            (":timestamp", &timestamp),
        ];
        binds.extend(fence.binds());
        tx.prepare_cached(&publish_sql)?.execute(&binds[..])?
    };
    if changed != 1 {
        return Ok(false);
    }
    if let Some(local_artifact_id) = local_artifact_id {
        upsert_suspend_local_artifact(&tx, &sandbox_id_string, local_artifact_id, local_path)?;
    }
    // Retiring the episode shares the claim predicate rather than restating a
    // subset of it. The previous hand-written version dropped `source` and the
    // lease guard, and pinned a hardcoded reason code instead of the claim's, so
    // it could match nothing while the caller still reported success — leaving a
    // quarantine row behind a sandbox now marked `suspended`, which the candidate
    // scan can never see again because it filters on quarantine-bearing states.
    let retire_fence = ExactClaimFence::new(
        &tx,
        claimed,
        timestamp,
        LeaseRequirement::MustBeLive,
        SandboxStateRequirement::AnyState,
        CleanupRequirement::MayExist,
    )?;
    let retire_sql = format!(
        "DELETE FROM sandbox_quarantine_state WHERE {}",
        retire_fence.quarantine_row_predicate(""),
    );
    let retired = tx
        .prepare_cached(&retire_sql)?
        .execute(&retire_fence.binds()[..])?;
    if retired != 1 {
        // Unreachable as the two statements stand: this fence relaxes the cleanup
        // guard the update above requires and pins the same identity conjuncts at
        // the same timestamp inside one transaction, so a row the update matched
        // this one matches too. The check is here for the divergence, not for a
        // race — tighten this predicate relative to the update's and the two stop
        // agreeing, and the sandbox update would commit without the episode
        // retirement, which is the orphaned-row state described above. Abandoning
        // the transaction lets the next pass retry under a fresh claim.
        return Ok(false);
    }
    retire_restore_transfer_scope(&tx, &sandbox_id_string)?;
    tx.commit()?;
    emit_sandbox_state_event(
        &sandbox_id_string,
        SandboxEventType::QuarantineRecovered,
        Some(SandboxState::Quarantined.as_str()),
        reason_code.as_str(),
        "publish_orphan_quarantine_recovered_artifact",
        SandboxState::Suspended,
        None,
    );
    Ok(true)
}

/// Proof that a claim is on the adoption branch, carrying the artifact id the
/// eligibility gate validated for it.
///
/// Constructible only inside this module, and only by the gate, so it exists exactly
/// where the evidence was checked.
///
/// The adoption statement used to take a bare `LocalArtifactId` and check it against
/// the claim's branch, which left two problems: a caller could pass an id the gate
/// never approved, and the refusal for a restore-branch claim was an arm no test
/// could reach, because the only caller destructures the id out of the adoption
/// branch immediately before calling. Deriving the id from the branch closes both —
/// a wrong branch cannot be constructed and a wrong id cannot be supplied, so
/// neither needs a runtime check or a reason code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdoptableArtifact(LocalArtifactId);

impl AdoptableArtifact {
    pub const fn local_artifact_id(self) -> LocalArtifactId {
        self.0
    }
}

/// What the adoption statement did.
///
/// Named rather than a `bool` so the caller's disposition reads at the call site:
/// an adoption is terminal for the candidate, a fence miss is a race to back off
/// from. `AdoptableArtifact` removes the third case a bare id needed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuarantineAdoptionOutcome {
    Adopted,
    /// The fenced CAS matched nothing: a cleanup owner arrived, the lease lapsed,
    /// or one of the pinned identity conjuncts moved. Retryable.
    FenceChanged,
}

/// Adopt one policy-approved quarantine into the durable compensation worker.
/// The caller has already validated the canonical artifact; this CAS binds
/// that evidence to the current local quarantine episode and artifact pin.
///
/// The artifact comes in as an `AdoptableArtifact`, which only the adoption branch
/// can produce, so this statement cannot be handed a restore-branch claim or an id
/// the gate never approved.
pub fn adopt_quarantine_cleanup_episode(
    conn: &Connection,
    claimed: &ClaimedQuarantineRecoveryCandidate,
    adoptable: AdoptableArtifact,
    execution_generation: ExecutionGeneration,
    cleanup_id: SandboxCleanupEpisodeId,
    execution: &ExecutionMetadata,
    local_path: &str,
) -> Result<QuarantineAdoptionOutcome> {
    let candidate = &claimed.candidate;
    let sandbox_id = candidate.sandbox_id.to_string();
    let local_artifact_id = adoptable.local_artifact_id();
    let timestamp = now_epoch_i64()?;
    let message = "automatic recovery adopted validated suspend evidence";
    // `state = 'quarantined'` is stricter than the fence's activatable check, so
    // the fence does not repeat it.
    let fence = ExactClaimFence::new(
        conn,
        claimed,
        timestamp,
        LeaseRequirement::MustBeLive,
        SandboxStateRequirement::AnyState,
        CleanupRequirement::MustBeAbsent,
    )?;
    let timestamp_text = timestamp.to_string();
    let local_artifact_id_text = local_artifact_id.to_string();
    let generation = execution_generation.get();
    let adopt_sql = format!(
        "UPDATE sandboxes
            SET state = 'cleanup_required',
                cleanup_completion = 'mark_suspended',
                cleanup_keep_filesystem = TRUE,
                space_api_route_execution_generation = :generation,
                failure_reason = :message,
                state_changed_at = CASE
                    WHEN state != 'cleanup_required' THEN :timestamp
                    ELSE state_changed_at
                END,
                updated_at = :timestamp
          WHERE id = :fence_sandbox_id
            AND state = 'quarantined'
            AND current_local_artifact_id = :local_artifact_id
            AND current_local_artifact_kind = 'full'
            AND current_local_artifact_origin = 'suspend'
            AND (space_api_route_execution_generation IS NULL
                 OR space_api_route_execution_generation = :generation)
            AND EXISTS (
                SELECT 1 FROM sandbox_quarantine_state qs
                 WHERE {}
            ){}",
        fence.quarantine_row_predicate("qs."),
        fence.sandbox_guards("sandboxes.id"),
    );
    let tx = immediate_transaction(conn)?;
    let changed = {
        let mut binds: Vec<(&str, &dyn rusqlite::ToSql)> = vec![
            (":generation", &generation),
            (":message", &message),
            (":timestamp", &timestamp_text),
            (":local_artifact_id", &local_artifact_id_text),
        ];
        binds.extend(fence.binds());
        tx.prepare_cached(&adopt_sql)?.execute(&binds[..])?
    };
    if changed != 1 {
        tx.commit()?;
        return Ok(QuarantineAdoptionOutcome::FenceChanged);
    }
    upsert_sandbox_execution_metadata_in_transaction(&tx, &sandbox_id, execution)?;
    upsert_suspend_local_artifact(&tx, &sandbox_id, local_artifact_id, local_path)?;
    // Runs inside the transaction that just committed the sandbox to cleanup, so
    // it deliberately does not re-check the environment it releases into: the
    // cleanup episode this function inserts below is expected to exist.
    let release_fence = ExactClaimFence::new(
        &tx,
        claimed,
        timestamp,
        LeaseRequirement::MustBeLive,
        SandboxStateRequirement::AnyState,
        CleanupRequirement::MayExist,
    )?;
    let release_sql = format!(
        "UPDATE sandbox_quarantine_state
            SET claim_token = NULL, claim_expires_at = NULL, updated_at = :timestamp
          WHERE {}",
        release_fence.quarantine_row_predicate(""),
    );
    {
        let mut binds: Vec<(&str, &dyn rusqlite::ToSql)> = vec![(":timestamp", &timestamp_text)];
        binds.extend(release_fence.binds());
        // The count matters here as much as it does in the publisher, and for the
        // same reason: this release is fenced, so zero rows means the fence no
        // longer holds. Discarding it would leave the lease behind while the
        // cleanup episode below is inserted anyway — an adopted episode whose
        // claim never came off, unclaimable until the lease lapses. The earlier
        // `changed != 1` guard covers the `sandboxes` write, not this one.
        let released = tx.prepare_cached(&release_sql)?.execute(&binds[..])?;
        if released != 1 {
            return Ok(QuarantineAdoptionOutcome::FenceChanged);
        }
    }
    tx.execute(
        "INSERT INTO sandbox_cleanup_episodes (
             sandbox_id, cleanup_id, completion, keep_filesystem, phase,
             entered_from_state, activation_state, requested_at,
             quarantine_reason_code, failure_message, created_at,
             recovery_local_artifact_id, recovery_execution_generation
         ) VALUES (
             ?1, ?2, 'mark_suspended', TRUE, 'requested', 'quarantined',
             'restoring', ?3, ?4, ?5, ?3, ?6, ?7
         )",
        params![
            sandbox_id,
            cleanup_id.to_string(),
            timestamp,
            candidate.reason_code.as_str(),
            message,
            local_artifact_id.to_string(),
            execution_generation.get(),
        ],
    )?;
    tx.commit()?;
    emit_sandbox_state_event(
        &sandbox_id,
        SandboxEventType::CleanupRequested,
        Some(SandboxState::Quarantined.as_str()),
        candidate.reason_code.as_str(),
        "adopt_quarantine_cleanup_episode",
        SandboxState::CleanupRequired,
        Some(message),
    );
    Ok(QuarantineAdoptionOutcome::Adopted)
}
