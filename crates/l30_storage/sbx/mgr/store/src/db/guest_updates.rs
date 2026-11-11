//! Durable admission, execution fences and bounded activation allowance.
//! The manager serializes capture and fences each restarting/committing RPC;
//! these guarded writes recheck authority after awaits. A receipt never grants
//! authority over a stopped, cleaning or replacement execution.
use super::*;
use pplx_space_spaced_update::{Attempt, Selection, policy::Promotion};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    Apply {
        promotion: Box<Promotion>,
        authorization: Uuid,
    },
    Restart,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Intent {
    pub operation: Uuid,
    pub sandbox: SandboxId,
    pub execution: SandboxActivationAttemptId,
    pub action: Action,
    pub force: bool,
    pub configuration_revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuestIdentity {
    pub boot: Uuid,
    pub incarnation: Uuid,
    pub committed: Selection,
    pub attempt: Option<Attempt>,
    pub forward_ports: Vec<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    Pending,
    Staged,
    Activating,
    Settling,
    Complete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
    Committed,
    Restarted,
    Deferred,
    NotRequired,
    CandidateFailed,
    AwaitingHost,
    RetryExhausted,
    Cancelled,
    Superseded,
    RecoveryRequired,
}

macro_rules! vocabulary {
    ($ty:ident, $($variant:ident => $name:literal),+ $(,)?) => {
        impl $ty {
            pub const fn as_str(self) -> &'static str { match self { $(Self::$variant => $name),+ } }
        }
        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(self.as_str()) }
        }
        impl FromStr for $ty {
            type Err = InvalidVocabulary;
            fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
                match value { $($name => Ok(Self::$variant)),+, _ => Err(InvalidVocabulary) }
            }
        }
    };
}
#[derive(Debug, thiserror::Error)]
#[error("unknown guest update state")]
pub struct InvalidVocabulary;
vocabulary!(Phase, Pending => "pending", Staged => "staged", Activating => "activating", Settling => "settling", Complete => "complete");
vocabulary!(Outcome, Committed => "committed", Restarted => "restarted", Deferred => "deferred", NotRequired => "not_required", CandidateFailed => "candidate_failed", AwaitingHost => "awaiting_host", RetryExhausted => "retry_exhausted", Cancelled => "cancelled", Superseded => "superseded", RecoveryRequired => "recovery_required");

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseHistory {
    selected: Promotion,
    forward: Option<Promotion>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    pub intent: Intent,
    pub phase: Phase,
    pub prior: Option<GuestIdentity>,
    pub observed: Option<GuestIdentity>,
    pub deadline: i64,
    pub outcome: Option<Outcome>,
}

impl Record {
    /// Only meaningful after a durable guest abort: the original daemon and
    /// staged fence survived unchanged, so no candidate incarnation ran.
    pub fn unstarted_activation(&self, seen: &GuestIdentity) -> bool {
        let Action::Apply { promotion, .. } = &self.intent.action else {
            return false;
        };
        self.phase == Phase::Activating
            && self.prior.as_ref().is_some_and(|prior| {
                prior.boot == seen.boot
                    && prior.incarnation == seen.incarnation
                    && prior.committed == seen.committed
                    && prior.attempt == seen.attempt
                    && seen.attempt.as_ref().is_some_and(|attempt| {
                        attempt.operation == self.intent.operation
                            && attempt.execution.to_string() == self.intent.execution.to_string()
                            && attempt.boot == seen.boot
                            && attempt.incarnation == seen.incarnation
                            && attempt.target.digest == promotion.target
                            && attempt.configuration_revision == self.intent.configuration_revision
                    })
            })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Refusal {
    Missing,
    NotRunning,
    ExecutionChanged,
    PoolMember,
    CleanupActive,
    OtherOperation,
    ConflictingId,
    InvalidPolicy,
    Backoff,
    Exhausted,
    Quarantined,
}

#[derive(Debug)]
pub enum Admission {
    Accepted(Box<Record>),
    Refused(Refusal),
}

fn json(value: &impl Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(Error::to_sql_conversion)
}

fn decode(row: &rusqlite::Row<'_>) -> rusqlite::Result<Record> {
    fn error(e: impl std::error::Error + Send + Sync + 'static) -> rusqlite::Error {
        rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(e))
    }
    let intent = serde_json::from_str::<Intent>(&row.get::<_, String>(0)?).map_err(error)?;
    let phase = row.get::<_, String>(1)?.parse::<Phase>().map_err(error)?;
    let identity = |index| -> rusqlite::Result<Option<GuestIdentity>> {
        row.get::<_, Option<String>>(index)?
            .map(|v| serde_json::from_str(&v).map_err(error))
            .transpose()
    };
    let outcome = row
        .get::<_, Option<String>>(5)?
        .map(|v| v.parse().map_err(error))
        .transpose()?;
    Ok(Record {
        intent,
        phase,
        prior: identity(2)?,
        observed: identity(3)?,
        deadline: row.get(4)?,
        outcome,
    })
}

db_private_outside_tests! {
    fn record(conn: &Connection, operation: Uuid) -> Result<Option<Record>> {
        conn.query_row(
            "SELECT request_json, phase, prior_identity_json, observed_identity_json, deadline, outcome
             FROM guest_update_operations WHERE operation_id = ?1",
            params![operation.to_string()], decode,
        ).optional()
    }
}
db_private_outside_tests! {
    fn active(conn: &Connection, sandbox: &str) -> Result<Option<Record>> {
        conn.query_row(
            "SELECT request_json, phase, prior_identity_json, observed_identity_json, deadline, outcome
             FROM guest_update_operations WHERE sandbox_id = ?1 AND terminal = 0 ORDER BY operation_id LIMIT 1",
            params![sandbox], decode,
        ).optional()
    }
}
db_private_outside_tests! {
    fn unfinished(conn: &Connection, after: Option<Uuid>) -> Result<Vec<Record>> {
        let mut statement = conn.prepare(
            "SELECT request_json, phase, prior_identity_json, observed_identity_json, deadline, outcome
             FROM guest_update_operations WHERE terminal = 0 AND operation_id > ?1 ORDER BY operation_id LIMIT 128",
        )?;
        Ok(statement.query_map(params![after.map(|v| v.to_string()).unwrap_or_default()], decode)?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

db_private_outside_tests! {
    fn retry_ready(conn: &Connection, record: &Record) -> Result<bool> {
        let Action::Apply { authorization, .. } = &record.intent.action else { return Ok(true); };
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM guest_update_policy WHERE sandbox_id = ?1 AND execution_id = ?2 AND authorization_id = ?3 AND attempts < 2 AND quarantine_reason IS NULL AND retry_not_before <= ?4)",
            params![record.intent.sandbox.to_string(), record.intent.execution.to_string(), authorization.to_string(), jiff::Timestamp::now().as_second()],
            |row| row.get(0),
        )
    }
}

fn current(conn: &Connection, intent: &Intent) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sandboxes WHERE id = ?1 AND node_execution_id = ?2
           AND state = ?3 AND cleanup_completion IS NULL AND node_execution_retired = 0
           AND NOT EXISTS (SELECT 1 FROM sandbox_cleanup_episodes WHERE sandbox_id = ?1))",
        params![
            intent.sandbox.to_string(),
            intent.execution.to_string(),
            SandboxState::Started.as_str()
        ],
        |row| row.get(0),
    )
}

/// Idempotent admission and policy reconciliation are one transaction. Only a
/// new authorization from the configured host policy replenishes attempts;
/// the RPC caller cannot mint that authority by choosing a new operation ID.
pub fn admit(conn: &Connection, intent: Intent, deadline: i64) -> Result<Admission> {
    let tx = immediate_transaction(conn)?;
    if let Some(existing) = record(&tx, intent.operation)? {
        return Ok(if existing.intent == intent {
            Admission::Accepted(Box::new(existing))
        } else {
            Admission::Refused(Refusal::ConflictingId)
        });
    }
    let Some(row) = get_sandbox(&tx, &intent.sandbox.to_string())? else {
        return Ok(Admission::Refused(Refusal::Missing));
    };
    let refusal = if row.node_execution_id != Some(intent.execution) {
        Some(Refusal::ExecutionChanged)
    } else if !row.state.is_unowned(SandboxState::Started) {
        Some(Refusal::NotRunning)
    } else if row.pool_template.is_some() {
        Some(Refusal::PoolMember)
    } else if row.cleanup_completion.is_some() || cleanup_episode(&tx, &row.id)?.is_some() {
        Some(Refusal::CleanupActive)
    } else if active(&tx, &row.id)?.is_some() {
        Some(Refusal::OtherOperation)
    } else {
        None
    };
    if let Some(refusal) = refusal {
        return Ok(Admission::Refused(refusal));
    }
    if intent.operation.is_nil() || deadline <= jiff::Timestamp::now().as_second() {
        return Err(Error::invalid_query());
    }
    if let Action::Apply {
        promotion,
        authorization,
    } = &intent.action
    {
        let old: Option<(String, String, u32, i64, Option<String>)> = tx.query_row(
            "SELECT promotion_json, authorization_id, attempts, retry_not_before, quarantine_reason
             FROM guest_update_policy WHERE sandbox_id = ?1 AND execution_id = ?2",
            params![row.id, intent.execution.to_string()], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        ).optional()?;
        let previous: Option<ReleaseHistory> = old
            .as_ref()
            .map(|v| serde_json::from_str(&v.0))
            .transpose()
            .map_err(|e| Error::decode(0, Type::Text, e))?;
        if old
            .as_ref()
            .is_none_or(|(_, auth, _, _, _)| auth != &authorization.to_string())
        {
            // A new operation UUID cannot revive retired policy authority.
            // Receipts already retain this history until sandbox deletion.
            let consumed: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM guest_update_operations
                 WHERE sandbox_id = ?1 AND execution_id = ?2
                   AND json_extract(request_json, '$.action.authorization') = ?3)",
                params![
                    row.id,
                    intent.execution.to_string(),
                    authorization.to_string()
                ],
                |row| row.get(0),
            )?;
            if consumed {
                return Ok(Admission::Refused(Refusal::InvalidPolicy));
            }
        }
        if authorization.is_nil()
            || promotion
                .accept_after(
                    previous
                        .as_ref()
                        .and_then(|history| history.forward.as_ref()),
                )
                .is_err()
        {
            return Ok(Admission::Refused(Refusal::InvalidPolicy));
        }
        if let Some((_, auth, attempts, retry, quarantine)) = &old
            && auth == &authorization.to_string()
        {
            let refusal =
                if previous.as_ref().map(|history| &history.selected) != Some(promotion.as_ref()) {
                    Some(Refusal::InvalidPolicy)
                } else if quarantine.is_some() {
                    Some(Refusal::Quarantined)
                } else if *attempts >= 2 {
                    Some(Refusal::Exhausted)
                } else if *retry > jiff::Timestamp::now().as_second() {
                    Some(Refusal::Backoff)
                } else {
                    None
                };
            if let Some(refusal) = refusal {
                return Ok(Admission::Refused(refusal));
            }
        }
        let history = ReleaseHistory {
            selected: (**promotion).clone(),
            forward: if promotion.intent
                == pplx_space_spaced_update::policy::SelectionIntent::Upgrade
            {
                Some((**promotion).clone())
            } else {
                previous.and_then(|history| history.forward)
            },
        };
        // Replaying the same authorization preserves every allowance field.
        tx.execute(
            "INSERT INTO guest_update_policy (sandbox_id, execution_id, promotion_json, authorization_id, attempts, retry_not_before, quarantine_reason)
             VALUES (?1, ?2, ?3, ?4, 0, 0, NULL)
             ON CONFLICT(sandbox_id, execution_id) DO UPDATE SET promotion_json = excluded.promotion_json,
               authorization_id = excluded.authorization_id, attempts = 0, retry_not_before = 0, quarantine_reason = NULL
             WHERE guest_update_policy.authorization_id != excluded.authorization_id",
            params![row.id, intent.execution.to_string(), json(&history)?, authorization.to_string()],
        )?;
    }
    let now = jiff::Timestamp::now().as_second();
    tx.execute(
        "INSERT INTO guest_update_operations (operation_id, sandbox_id, execution_id, request_json, phase, deadline, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
        params![intent.operation.to_string(), row.id, intent.execution.to_string(), json(&intent)?, Phase::Pending.as_str(), deadline, now],
    )?;
    let result = record(&tx, intent.operation)?.ok_or_else(Error::no_rows)?;
    tx.commit()?;
    Ok(Admission::Accepted(Box::new(result)))
}

/// Charge before sending Arm/Restart. An ambiguous network result remains
/// Activating and is reconciled from the guest journal, never charged twice.
pub fn advance(
    conn: &Connection,
    record: &Record,
    to: Phase,
    observed: Option<&GuestIdentity>,
) -> Result<bool> {
    if !matches!(
        (record.phase, to),
        (Phase::Pending, Phase::Staged)
            | (Phase::Pending | Phase::Staged, Phase::Activating)
            | (Phase::Settling, Phase::Complete)
    ) {
        return Err(Error::invalid_query());
    }
    let tx = immediate_transaction(conn)?;
    if !current(&tx, &record.intent)?
        || crate::db::guest_updates::record(&tx, record.intent.operation)?.as_ref() != Some(record)
    {
        return Ok(false);
    }
    if to == Phase::Activating
        && let Action::Apply { authorization, .. } = &record.intent.action
        && tx.execute("UPDATE guest_update_policy SET attempts = attempts + 1 WHERE sandbox_id = ?1 AND execution_id = ?2 AND authorization_id = ?3 AND attempts < 2 AND quarantine_reason IS NULL AND retry_not_before <= ?4",
            params![record.intent.sandbox.to_string(), record.intent.execution.to_string(), authorization.to_string(), jiff::Timestamp::now().as_second()])? != 1 { return Ok(false); }
    // Requeue only the one ambiguous normal activation. Forced maintenance is
    // never repeated automatically: new customer work may have arrived since
    // the operator authorized interruption. Release is acknowledged before this
    // transition, so the backoff never holds customer traffic.
    if to == Phase::Complete
        && record.outcome == Some(Outcome::AwaitingHost)
        && !record.intent.force
        && let Action::Apply { authorization, .. } = &record.intent.action
    {
        let retry: Option<i64> = tx.query_row(
            "SELECT retry_not_before FROM guest_update_policy WHERE sandbox_id = ?1 AND execution_id = ?2 AND authorization_id = ?3 AND attempts < 2 AND quarantine_reason IS NULL",
            params![record.intent.sandbox.to_string(), record.intent.execution.to_string(), authorization.to_string()], |row| row.get(0),
        ).optional()?;
        if let Some(retry) = retry {
            tx.execute(
                "UPDATE guest_update_operations SET phase = ?2, prior_identity_json = NULL, observed_identity_json = NULL, outcome = NULL, deadline = ?3, updated_at = ?4 WHERE operation_id = ?1",
                params![record.intent.operation.to_string(), Phase::Pending.as_str(), retry.max(jiff::Timestamp::now().as_second()) + 360, jiff::Timestamp::now().as_second()],
            )?;
            tx.commit()?;
            return Ok(true);
        }
    }
    let identity = observed.map(json).transpose()?;
    tx.execute("UPDATE guest_update_operations SET phase = ?2, terminal = ?3, observed_identity_json = COALESCE(?4, observed_identity_json), prior_identity_json = CASE WHEN ?2 = ?5 THEN ?4 ELSE prior_identity_json END, updated_at = ?6 WHERE operation_id = ?1",
        params![record.intent.operation.to_string(), to.as_str(), to == Phase::Complete, identity, Phase::Activating.as_str(), jiff::Timestamp::now().as_second()])?;
    tx.commit()?;
    Ok(true)
}

/// Save the outcome before releasing the host hold. Settling survives a crash
/// after the release and can finish idempotently. AwaitingHost is deliberately
/// an uncertain cause: one delayed retry, not an assertion the candidate works.
pub fn settle(
    conn: &Connection,
    record: &Record,
    outcome: Outcome,
    observed: Option<&GuestIdentity>,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    if record.phase == Phase::Complete
        || record.phase == Phase::Settling
        || !current(&tx, &record.intent)?
        || crate::db::guest_updates::record(&tx, record.intent.operation)?.as_ref() != Some(record)
    {
        return Ok(false);
    }
    let mut outcome = outcome;
    if let Action::Apply { authorization, .. } = &record.intent.action {
        // The caller has durably aborted the staged fence before settling a
        // deferral. Refund only the unchanged original daemon; unknown RPC
        // results and later incarnations retain their conservative charge.
        if outcome == Outcome::Deferred
            && observed.is_some_and(|seen| record.unstarted_activation(seen))
            && tx.execute(
                "UPDATE guest_update_policy SET attempts = attempts - 1
                 WHERE sandbox_id = ?1 AND execution_id = ?2 AND authorization_id = ?3 AND attempts > 0",
                params![record.intent.sandbox.to_string(), record.intent.execution.to_string(), authorization.to_string()],
            )? != 1
        {
            return Ok(false);
        }
        if outcome == Outcome::AwaitingHost && !record.intent.force {
            let attempts: u32 = tx.query_row(
                "SELECT attempts FROM guest_update_policy WHERE sandbox_id = ?1 AND execution_id = ?2 AND authorization_id = ?3",
                params![record.intent.sandbox.to_string(), record.intent.execution.to_string(), authorization.to_string()],
                |row| row.get(0),
            )?;
            if attempts >= 2 {
                outcome = Outcome::RetryExhausted;
            }
        }
        let quarantine = matches!(
            outcome,
            Outcome::CandidateFailed | Outcome::RecoveryRequired
        )
        .then_some(outcome.as_str());
        // Lifecycle cancellation before activation has not restarted anything.
        // Preserve any older backoff through MAX below, but do not introduce
        // a new delay for an untouched or staged operation.
        let retry = if outcome == Outcome::AwaitingHost
            || (outcome == Outcome::Cancelled && record.phase == Phase::Activating)
        {
            jiff::Timestamp::now().as_second()
                + 300
                + (record.intent.operation.as_u128() % 61) as i64
        } else {
            0
        };
        tx.execute("UPDATE guest_update_policy SET quarantine_reason = COALESCE(?4, quarantine_reason), retry_not_before = MAX(retry_not_before, ?5) WHERE sandbox_id = ?1 AND execution_id = ?2 AND authorization_id = ?3",
            params![record.intent.sandbox.to_string(), record.intent.execution.to_string(), authorization.to_string(), quarantine, retry])?;
    }
    tx.execute("UPDATE guest_update_operations SET phase = ?2, outcome = ?3, observed_identity_json = COALESCE(?4, observed_identity_json), updated_at = ?5 WHERE operation_id = ?1",
        params![record.intent.operation.to_string(), Phase::Settling.as_str(), outcome.as_str(), observed.map(json).transpose()?, jiff::Timestamp::now().as_second()])?;
    tx.commit()?;
    Ok(true)
}

/// Stop/cleanup or replacement owns the old execution's resources. Paused
/// executions retain an undecided receipt for reconciliation after resume;
/// retirement never requires contacting a guest or clearing its admission.
pub fn supersede(conn: &Connection, operation: Uuid) -> Result<bool> {
    Ok(conn.execute("UPDATE guest_update_operations SET phase = ?2, terminal = 1, outcome = ?3, updated_at = ?4
        WHERE operation_id = ?1 AND terminal = 0 AND (
          NOT EXISTS (SELECT 1 FROM sandboxes WHERE sandboxes.id = guest_update_operations.sandbox_id
                      AND sandboxes.node_execution_id = guest_update_operations.execution_id)
          OR EXISTS (SELECT 1 FROM sandboxes WHERE sandboxes.id = guest_update_operations.sandbox_id
                     AND (unowned_state IN (?5, ?6, ?7) OR cleanup_state IS NOT NULL OR cleanup_completion IS NOT NULL OR node_execution_retired = 1)))",
        params![operation.to_string(), Phase::Complete.as_str(), Outcome::Superseded.as_str(), jiff::Timestamp::now().as_second(), SandboxState::Stopping.as_str(), SandboxState::Stopped.as_str(), SandboxState::Deleting.as_str()])? == 1)
}

#[cfg(test)]
mod tests;
